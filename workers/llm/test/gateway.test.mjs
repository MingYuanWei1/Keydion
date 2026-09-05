import assert from 'node:assert/strict';
import { test } from 'node:test';
import { handle } from '../src/index.ts';

function environment() {
  const route = {enabled: true, base_url: 'https://provider.example/v1', model: 'actual-model', key_secret: 'CHAT_API_KEY'};
  return {
    KEYDION_TOKEN: 'worker-secret', CHAT_API_KEY: 'provider-secret', VISION_API_KEY: 'vision-secret', EMBED_API_KEY: 'embed-secret',
    MODEL_ROUTES: {flash: {...route}, think: {...route, model: 'reasoner'}, vision: {...route, key_secret: 'VISION_API_KEY'},
      embed: {...route, key_secret: 'EMBED_API_KEY', model: 'embed-v1', dimensions: 2}},
  };
}
function req(body, path='/v1/chat/completions', token='worker-secret', headers={}) {
  return new Request('https://worker.example' + path, {
    method: body === undefined ? 'GET' : 'POST',
    headers: {'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json', ...headers},
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}
async function caps(env) { return (await (await handle(req(undefined, '/v1/capabilities'), env)).json()).purposes; }
const forbiddenFetch = () => { throw new Error('must not contact provider'); };

test('authentication protects both inference and capability discovery', async () => {
  for (const request of [req({model:'flash'}, undefined, 'bad'), req(undefined, '/v1/capabilities', 'bad')]) {
    const response = await handle(request, environment(), forbiddenFetch);
    assert.equal(response.status, 401);
    assert.equal((await response.json()).error.code, 'unauthorized');
  }
  const env = environment(); env.KEYDION_TOKEN = '';
  assert.equal((await handle(req({model:'flash'}, undefined, ''), env, forbiddenFetch)).status, 401);
});

test('discovery is independent, read-only, and contains no credentials or endpoints', async () => {
  const env = environment(); env.MODEL_ROUTES.flash.enabled = false; env.VISION_API_KEY = '';
  const result = await caps(env);
  assert.equal(result.flash.enabled, false);
  assert.equal(result.think.enabled, true);
  assert.equal(result.vision.enabled, false);
  assert.match(result.embed.embedding_id, /^[a-f0-9]{64}$/);
  assert.doesNotMatch(JSON.stringify(result), /secret|provider.example/);
});

test('chat, tools, image content and JSON output survive forwarding; provider receives only its own key', async () => {
  for (const purpose of ['flash','think','vision']) {
    const env = environment();
    const payload = {model:purpose, messages:[{role:'user',content:[{type:'image_url',image_url:{url:'data:image/png;base64,abc'}}]}],
      tools:[{type:'function',function:{name:'search',parameters:{type:'object'}}}], tool_choice:'auto', response_format:{type:'json_object'}, temperature:0.1};
    const response = await handle(req(payload), env, async (url, options) => {
      assert.equal(url, 'https://provider.example/v1/chat/completions');
      assert.equal(options.redirect, 'manual');
      assert.deepEqual(options.headers, {'Authorization':`Bearer ${purpose === 'vision' ? 'vision-secret' : 'provider-secret'}`, 'Content-Type':'application/json'});
      assert.deepEqual(JSON.parse(options.body), {...payload, model: env.MODEL_ROUTES[purpose].model});
      return Response.json({choices:[{message:{content:'{"ok":true}'}}]}, {headers:{'set-cookie':'private=secret'}});
    });
    assert.equal(response.headers.get('set-cookie'), null);
    assert.equal(response.headers.get('cache-control'), 'no-store');
    assert.equal((await response.json()).choices[0].message.content, '{"ok":true}');
  }
});

test('SSE is forwarded before completion, including tool deltas and DONE', async () => {
  let finish;
  const first = 'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{"}}]}}]}\n\n';
  const upstream = new ReadableStream({start(c) { c.enqueue(new TextEncoder().encode(first)); finish=()=>{c.enqueue(new TextEncoder().encode('data: [DONE]\n\n'));c.close();}; }});
  const response = await handle(req({model:'think',stream:true}), environment(), async()=>new Response(upstream));
  const reader = response.body.getReader();
  assert.equal(new TextDecoder().decode((await reader.read()).value), first);
  assert.equal(response.headers.get('content-type'), 'text/event-stream');
  finish();
  assert.equal(new TextDecoder().decode((await reader.read()).value), 'data: [DONE]\n\n');
  assert.equal((await reader.read()).done, true);
});

test('route reasoning replaces the legacy provider-specific thinking flag', async () => {
  const env=environment(); env.MODEL_ROUTES.think.reasoning_effort='high';
  const response=await handle(req({model:'think',thinking:{type:'enabled'},messages:[]}),env,async(_url,options)=>{
    const payload=JSON.parse(options.body);
    assert.equal(payload.reasoning_effort,'high');
    assert.equal('thinking' in payload,false);
    return Response.json({choices:[]});
  });
  assert.equal(response.status,200);
  await response.text();
});

test('consumer cancellation aborts upstream and releases its stream', async () => {
  let signal; let cancelled=false;
  const response = await handle(req({model:'flash',stream:true}), environment(), async(_url, options)=>{
    signal=options.signal;
    return new Response(new ReadableStream({cancel(){cancelled=true;}}));
  });
  await response.body.cancel();
  assert.equal(signal.aborted, true);
  assert.equal(cancelled, true);
});

test('disabled, arbitrary-model and wrong-path requests never reach a provider', async () => {
  const env=environment(); env.MODEL_ROUTES.flash.enabled=false;
  for (const [request, status] of [[req({model:'flash'}),503],[req({model:'arbitrary'}),400],
    [req({model:'embed'}),400],[req({model:'think'},'/v1/models'),404],[req(undefined),405]]) {
    assert.equal((await handle(request,env,forbiddenFetch)).status,status);
  }
});

test('upstream failures and redirects are sanitized without fallback', async () => {
  for (const status of [302,400,401,429,500]) {
    let calls=0;
    const response=await handle(req({model:'flash'}),environment(),async()=>{
      calls++;
      return new Response('provider-secret private prompt',{status,headers:{location:'https://other.example','set-cookie':'secret'}});
    });
    assert.equal(calls,1);
    assert.equal(response.status,status===302?502:status);
    assert.equal(response.headers.get('location'),null);
    assert.doesNotMatch(await response.text(),/provider-secret|private prompt/);
  }
});

test('provider timeout returns an explicit error', async () => {
  const env=environment(); env.MODEL_ROUTES.flash.timeout_ms=5;
  const response=await handle(req({model:'flash'}),env,async(_url,{signal})=>new Promise((resolve,reject)=>{
    signal.addEventListener('abort',()=>reject(new Error('provider private error')));
  }));
  assert.equal(response.status,502);
  assert.equal((await response.json()).error.code,'upstream_unavailable');
});

test('embedding pin covers endpoint, model and dimensions; mismatch blocks before inference', async () => {
  const env=environment(); const pin=(await caps(env)).embed.embedding_id;
  for (const field of ['model','base_url','dimensions']) {
    const changed=environment(); changed.MODEL_ROUTES.embed[field] = field==='dimensions'?3:`${changed.MODEL_ROUTES.embed[field]}-new`;
    const response=await handle(req({model:`embed:${pin}`,input:['q']},'/v1/embeddings','worker-secret',{'x-keydion-embed-dim':'2'}),changed,forbiddenFetch);
    assert.equal(response.status,409);
  }
  assert.equal((await handle(req({model:`embed:${pin}`,input:['q']},'/v1/embeddings'),env,forbiddenFetch)).status,409);
});

test('embedding vectors are validated and SDK base64 preference becomes float', async () => {
  const env=environment(); const pin=(await caps(env)).embed.embedding_id;
  for (const [data,status] of [[[{index:0,embedding:[0.1,0.2]}],200],[[{index:0,embedding:[0.1]}],502],[[{index:2,embedding:[0.1,0.2]}],502],[[],502]]) {
    const response=await handle(req({model:`embed:${pin}`,input:['q'],encoding_format:'base64'},'/v1/embeddings','worker-secret',{'x-keydion-embed-dim':'2'}),env,async(url,options)=>{
      assert.equal(url,'https://provider.example/v1/embeddings');
      assert.equal(JSON.parse(options.body).encoding_format,'float');
      assert.equal(JSON.parse(options.body).model,'embed-v1');
      return Response.json({data,model:'embed-v1'});
    });
    assert.equal(response.status,status);
    await response.text();
  }
});

test('structured logs never contain request or response content', async () => {
  const logs=[]; const original=console.log; console.log=(line)=>logs.push(JSON.parse(line));
  try {
    const response=await handle(req({model:'flash',messages:[{role:'user',content:'private question'}]}),environment(),async()=>Response.json({answer:'private answer'}));
    await response.text();
    assert.equal(logs.length,1);
    assert.deepEqual(Object.keys(logs[0]).sort(),['duration_ms','model','purpose','request_id','status']);
    assert.doesNotMatch(JSON.stringify(logs),/secret|private/);
  } finally {console.log=original;}
});

test('Google embedding batches restore an omitted zero index without masking other missing indices', async () => {
  const env=environment();
  env.MODEL_ROUTES.embed.base_url='https://generativelanguage.googleapis.com/v1beta/openai';
  const pin=(await caps(env)).embed.embedding_id;
  for (const [data,status] of [
    [[{embedding:[0.1,0.2]},{index:1,embedding:[0.3,0.4]}],200],
    [[{index:null,embedding:[0.1,0.2]},{index:1,embedding:[0.3,0.4]}],200],
    [[{embedding:[0.1,0.2]},{embedding:[0.3,0.4]}],502],
  ]) {
    const response=await handle(req({model:`embed:${pin}`,input:['English','中文']},'/v1/embeddings','worker-secret',{'x-keydion-embed-dim':'2'}),env,async()=>Response.json({data}));
    assert.equal(response.status,status);
    const result=await response.json();
    if (status===200) {
      assert.deepEqual(result.data.map(row=>row.index),[0,1]);
      assert.deepEqual(result.data.map(row=>row.embedding),[[0.1,0.2],[0.3,0.4]]);
    }
  }
});

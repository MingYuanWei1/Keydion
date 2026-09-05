// Exercise the actual Workers runtime as well as the unit-level forwarding tests.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { build } from 'esbuild';
import { Miniflare, convertV4MiniflareOptions } from 'miniflare';

test('workerd serves authenticated capabilities and forwards streaming chat', async () => {
  const bundle = await build({entryPoints:[new URL('../src/index.ts',import.meta.url).pathname],
    bundle:true, format:'esm', write:false, external:['node:crypto']});
  const route={enabled:true,base_url:'https://provider.example/v1',model:'actual-model',key_secret:'CHAT_API_KEY'};
  const requests=[];
  const mf=new Miniflare(convertV4MiniflareOptions({
    rootPath:new URL('..',import.meta.url).pathname,
    modules:true, script:bundle.outputFiles[0].text,
    compatibilityDate:'2026-09-05', compatibilityFlags:['nodejs_compat'],
    bindings:{KEYDION_TOKEN:'test-token',CHAT_API_KEY:'provider-token',MODEL_ROUTES:{
      flash:route,think:route,vision:route,embed:{...route,dimensions:2},
    }},
    outboundService:async(request)=>{
      requests.push(request);
      assert.equal(request.headers.get('authorization'),'Bearer provider-token');
      const payload=await request.json();
      assert.equal(payload.model,'actual-model');
      return new Response('data: {"choices":[{"delta":{"content":"hello"}}]}\n\ndata: [DONE]\n\n', {headers:{'content-type':'text/event-stream'}});
    },
  }));
  try {
    assert.equal((await mf.dispatchFetch('https://worker.example/v1/capabilities')).status,401);
    const caps=await mf.dispatchFetch('https://worker.example/v1/capabilities',{headers:{authorization:'Bearer test-token'}});
    assert.equal((await caps.json()).purposes.flash.enabled,true);
    const response=await mf.dispatchFetch('https://worker.example/v1/chat/completions',{
      method:'POST',headers:{authorization:'Bearer test-token','content-type':'application/json'},
      body:JSON.stringify({model:'flash',stream:true,messages:[{role:'user',content:'hello'}]}),
    });
    assert.equal(response.status,200);
    assert.match(await response.text(),/hello.*\n\ndata: \[DONE\]/);
    assert.equal(requests.length,1);
  } finally {await mf.dispose();}
});

"""Deployment contract for Paper publishing operations and CI."""

from __future__ import annotations

import hashlib
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "test.yml"
WEB_UNIT = ROOT / "deploy" / "keydion.service"
WORKER_UNIT = ROOT / "deploy" / "keydion-publishing-worker.service"
RUNBOOK = ROOT / "docs" / "deployment" / "paper-publishing-migration.md"

FOCUSED_PUBLISHING_TESTS = (
    "tests/test_publishing_contracts.py",
    "tests/test_publishing_models.py",
    "tests/test_alembic_runtime.py",
    "tests/test_publishing_migration.py",
    "tests/test_paper_storage.py",
    "tests/test_publishing_publish.py",
    "tests/test_publishing_submissions.py",
    "tests/test_publishing_revisions.py",
    "tests/test_publishing_delete.py",
    "tests/test_publishing_jobs.py",
    "tests/test_publishing_worker.py",
    "tests/test_rag_revision_identity.py",
    "tests/test_publishing_http.py",
    "tests/test_paper_library.py",
    "tests/test_paper_identity_routes.py",
    "tests/test_publishing_mutation_routes.py",
    "tests/test_publishing_revision_ui.py",
    "tests/test_publishing_legacy_writers.py",
    "tests/test_paper_id_consumers.py",
    "tests/test_publishing_i18n.py",
    "tests/test_publishing_deployment_contract.py",
)

WORKER_DEFAULTS = (
    "PAPERQUERY_PUBLISHING_WORKER_POLL_SECONDS=5",
    "PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS=1800",
    "PAPERQUERY_PUBLISHING_INLINE_INDEX_TIMEOUT_SECONDS=45",
    "PAPERQUERY_PUBLISHING_RESERVATION_GRACE_SECONDS=3600",
)


def _parse_unit(text: str) -> dict[str, dict[str, str]]:
    sections: dict[str, dict[str, str]] = {}
    current: dict[str, str] | None = None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line.startswith("[") and line.endswith("]"):
            current = sections.setdefault(line[1:-1], {})
            continue
        if current is not None and "=" in line:
            key, value = line.split("=", 1)
            current[key] = value
    return sections


class PublishingDeploymentContract(unittest.TestCase):
    def _required_text(self, path: Path) -> str:
        self.assertTrue(path.is_file(), f"required deployment file is missing: {path}")
        return path.read_text(encoding="utf-8")

    def _assert_shared_service_contract(self, path: Path) -> dict[str, dict[str, str]]:
        unit = _parse_unit(self._required_text(path))
        self.assertIn("Service", unit)
        self.assertIn("Install", unit)
        service = unit["Service"]
        self.assertEqual("keydion", service.get("User"))
        self.assertEqual("keydion", service.get("Group"))
        self.assertEqual("/Keydion", service.get("WorkingDirectory"))
        self.assertEqual("/Keydion/.env.prod", service.get("EnvironmentFile"))
        self.assertTrue(
            service.get("ExecStart", "").startswith("/Keydion/.venv/bin/python ")
        )
        writable = service.get("ReadWritePaths", "").split()
        self.assertIn("/Keydion/papers", writable)
        self.assertIn("/Keydion/data/pending_papers", writable)
        self.assertEqual("0027", service.get("UMask"))
        self.assertEqual("SIGTERM", service.get("KillSignal"))
        self.assertEqual("on-failure", service.get("Restart"))
        self.assertEqual("multi-user.target", unit["Install"].get("WantedBy"))
        return unit

    def test_workflow_pins_mysql_and_keeps_generated_urls_child_only(self):
        workflow = self._required_text(WORKFLOW)
        self.assertGreaterEqual(
            len(re.findall(r"python-version:\s*['\"]?3\.11['\"]?", workflow)),
            2,
        )
        self.assertIn("image: mysql:9.7.1", workflow)
        for health_option in (
            "--health-cmd",
            "mysqladmin ping",
            "--health-interval",
            "--health-timeout",
            "--health-retries",
        ):
            self.assertIn(health_option, workflow)
        self.assertIn("PAPERQUERY_TEST_MYSQL_ADMIN_URL", workflow)
        self.assertNotIn("PAPERQUERY_DATABASE_URL", workflow)
        self.assertNotIn("PAPERQUERY_TEST_MYSQL_URL", workflow)
        self.assertNotIn("MYSQL_DATABASE", workflow)
        self.assertIn("python3 -m unittest", workflow)
        self.assertGreaterEqual(workflow.count("tools/run_isolated_tests.py"), 3)
        for target in (
            "tests/test_alembic_runtime.py",
            "tests/test_publishing_migration.py",
            "tests/test_publishing_migration_mysql.py",
            "tests/test_publishing_mysql_concurrency.py",
            *FOCUSED_PUBLISHING_TESTS,
        ):
            self.assertIn(target, workflow)
        self.assertRegex(
            workflow,
            r'tools/run_isolated_tests\.py\s+discover\s+-s\s+tests\s+-p\s+["\']test_\*\.py["\']\s+-v',
        )

        runner = (ROOT / "tools" / "run_isolated_tests.py").read_text(
            encoding="utf-8"
        )
        self.assertIn('database_name = f"keydion_test_{uuid.uuid4().hex}"', runner)
        self.assertIn('"PAPERQUERY_DATABASE_URL": test_url.render_as_string', runner)
        self.assertIn('"PAPERQUERY_TEST_MYSQL_URL": test_url.render_as_string', runner)
        self.assertGreaterEqual(runner.count("test_url.render_as_string"), 2)

    def test_web_unit_uses_the_tracked_host_contract(self):
        unit = self._assert_shared_service_contract(WEB_UNIT)
        service = unit["Service"]
        self.assertEqual(
            "/Keydion/.venv/bin/python -m gunicorn -c "
            "/Keydion/gunicorn.conf.py app:app",
            service.get("ExecStart"),
        )
        self.assertEqual("/bin/kill -HUP $MAINPID", service.get("ExecReload"))

    def test_worker_unit_is_exact_and_independently_enabled(self):
        unit = self._assert_shared_service_contract(WORKER_UNIT)
        self.assertEqual(
            "/Keydion/.venv/bin/python -m tools.publishing_worker",
            unit["Service"].get("ExecStart"),
        )
        relationships = " ".join(
            unit.get("Unit", {}).get(key, "")
            for key in ("Requires", "BindsTo", "PartOf")
        )
        self.assertNotIn("keydion.service", relationships)

    def test_gunicorn_only_warms_the_snapshot_and_compose_is_unchanged(self):
        gunicorn = (ROOT / "gunicorn.conf.py").read_text(encoding="utf-8")
        post_fork = gunicorn[gunicorn.index("def post_fork"):]
        self.assertIn("rag_index.warm()", post_fork)
        self.assertNotIn("publishing_worker", post_fork)
        self.assertNotIn("build_publishing_worker", post_fork)

        compose = (ROOT / "docker-compose.prod.yml").read_bytes()
        self.assertEqual(
            "06fd4b8a3d397470b49395f981bfe2410b0cc7392a0bda605acfe85e517fa8af",
            hashlib.sha256(compose).hexdigest(),
        )

    def test_environment_documents_the_approved_worker_defaults(self):
        env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
        for setting in WORKER_DEFAULTS:
            self.assertEqual(1, env_example.count(setting), setting)

    def test_runbook_prescribes_the_safe_order_and_rollback_boundary(self):
        runbook = self._required_text(RUNBOOK)
        lower = runbook.casefold()
        ordered_sections = (
            "## 2. stop the worker and web service",
            "## 3. take coordinated database and filesystem backups",
            "## 4. run the read-only preflight",
            "## 5. stamp only a validated legacy baseline",
            "## 6. upgrade or safely resume",
            "## 7. validate the migrated state",
            "## 8. smoke-test the lifecycle",
            "## 9. restart worker and web, then reopen traffic",
        )
        positions = [lower.index(section) for section in ordered_sections]
        self.assertEqual(sorted(positions), positions)

        self.assertIn("set -euo pipefail", runbook)
        self.assertNotIn('source "$KEYDION_ROOT/.env.prod"', runbook)
        self.assertNotIn("source /Keydion/.env.prod", runbook)
        self.assertIn('chown root:keydion "$KEYDION_ROOT/.env.prod"', runbook)
        self.assertIn("read_dotenv_value", runbook)
        self.assertIn("KEYDION_OLD_RELEASE", runbook)
        self.assertIn("KEYDION_NEW_RELEASE", runbook)
        self.assertIn(
            'printf \'%s\\n\' "$KEYDION_OLD_RELEASE"',
            runbook,
        )
        self.assertIn(
            'git -C "$KEYDION_ROOT" checkout --detach "$KEYDION_NEW_RELEASE"',
            runbook,
        )
        self.assertLess(
            runbook.index('printf \'%s\\n\' "$KEYDION_OLD_RELEASE"'),
            runbook.index(
                'git -C "$KEYDION_ROOT" checkout --detach '
                '"$KEYDION_NEW_RELEASE"'
            ),
        )
        self.assertIn("gzip -t", lower)
        self.assertGreaterEqual(lower.count("tar -tf"), 2)
        self.assertIn("@@global.server_uuid", lower)
        self.assertIn("database-identity.txt", lower)
        self.assertIn("keydion_application_db_identity", lower)
        self.assertIn("keydion_backup_db_identity", lower)
        self.assertIn("keydion_current_release", lower)
        self.assertIn(
            '"$keydion_old_release"|"$keydion_new_release"',
            lower,
        )
        self.assertIn("systemd/keydion.service.absent", lower)
        self.assertIn("systemd/keydion-publishing-worker.service.absent", lower)
        self.assertIn("paperquery_data_dir must be empty or /keydion/data", lower)
        self.assertIn(
            "paperquery_upload_dir must be empty or /keydion/papers",
            lower,
        )
        self.assertGreaterEqual(runbook.count("$KEYDION_PAPERS_DIR"), 5)
        self.assertGreaterEqual(runbook.count("$KEYDION_PENDING_DIR"), 5)

        for required in (
            "systemctl stop keydion-publishing-worker",
            "systemctl stop keydion",
            "mysqldump",
            "papers.tar",
            "pending-papers.tar",
            "tools/preflight_publishing_migration.py",
            "alembic stamp 0000_legacy_baseline",
            "paperquery_publishing_maintenance=1",
            "alembic upgrade head",
            "0003_publishing_contract (head)",
            "publishing_migration_issues",
            "source_sha256",
            "legacy_chunk_fingerprint",
            "vector_count",
            "/paper/<paper_id>",
            "/preview/<legacy_filename>",
            "%(paper_name)s uploaded successfully, but rag indexing failed.",
            "%(paper_name)s published successfully, but rag indexing failed.",
            "systemctl start keydion-publishing-worker",
            "systemctl start keydion",
            "submission_unmatched",
            "submission_ambiguous",
            "paper_id = null",
            "missing_pdf",
            "alias_collision",
            "duplicate_chunk",
            "one verified release",
            "out of scope",
        ):
            self.assertIn(required, lower)

        self.assertLess(
            lower.index("\nsudo systemctl start keydion-publishing-worker\n"),
            lower.index("\nsudo systemctl start keydion\n"),
        )
        rollback = lower[lower.index("## rollback"):]
        for required in (
            "old release",
            "database backup",
            "papers.tar",
            "pending-papers.tar",
            "together",
            "systemd/keydion.service",
            "systemd/keydion-publishing-worker.service",
            "systemctl daemon-reload",
        ):
            self.assertIn(required, rollback)

    def test_readme_and_context_describe_normal_operations_not_startup_migration(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        context = (ROOT / "CONTEXT.md").read_text(encoding="utf-8")
        production_docs = (readme + "\n" + context).casefold()
        for outdated_claim in (
            "creates and migrates all",
            "runs its idempotent `alter table` migrations",
            "there is no separate schema or migration step",
        ):
            self.assertNotIn(outdated_claim, production_docs)
        self.assertIn(
            "docs/deployment/paper-publishing-migration.md",
            readme,
        )
        self.assertIn("keydion-publishing-worker", readme)
        self.assertIn("not authoritative for production", readme.casefold())
        self.assertNotIn(
            "ci and the publishing migration are pinned to 9.7.1",
            readme.casefold(),
        )
        self.assertNotIn(
            "non-authoritative reference and is **not authoritative",
            readme.casefold(),
        )
        self.assertIn("**Publishing worker**", context)
        self.assertIn("**Publishing migration**", context)


if __name__ == "__main__":
    unittest.main()

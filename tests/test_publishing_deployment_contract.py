"""Deployment contract for Paper publishing operations and CI."""

from __future__ import annotations

import re
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "test.yml"
WEB_UNIT = ROOT / "deploy" / "keydion.service"
LEGACY_WEB_UNIT = (
    ROOT / "tests" / "fixtures" / "deploy" / "keydion-legacy.service.fixture"
)
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
            "/Keydion/gunicorn.conf.py wsgi:app",
            service.get("ExecStart"),
        )
        self.assertEqual("/bin/kill -HUP $MAINPID", service.get("ExecReload"))

    def test_legacy_web_unit_is_the_reviewed_first_rollout_fixture(self):
        self.assertEqual(
            """[Unit]
Description=Keydion (gunicorn)
After=network.target

[Service]
User=keydion
WorkingDirectory=/Keydion
ExecStart=/Keydion/run_prod.sh
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
KillSignal=SIGTERM
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
""",
            self._required_text(LEGACY_WEB_UNIT),
        )

    def test_worker_unit_is_exact_and_independently_enabled(self):
        unit = self._assert_shared_service_contract(WORKER_UNIT)
        service = unit["Service"]
        self.assertEqual(
            "/Keydion/.venv/bin/python -m tools.publishing_worker",
            service.get("ExecStart"),
        )
        lease_default = int(
            next(
                setting.split("=", 1)[1]
                for setting in WORKER_DEFAULTS
                if setting.startswith("PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS=")
            )
        )
        self.assertGreater(
            int(service.get("TimeoutStopSec", "0")),
            lease_default,
            "systemd must not SIGKILL a worker while its default lease is valid",
        )
        relationships = " ".join(
            unit.get("Unit", {}).get(key, "")
            for key in ("Requires", "BindsTo", "PartOf")
        )
        self.assertNotIn("keydion.service", relationships)

    def test_gunicorn_only_warms_snapshot_and_compose_runs_attachment_worker(self):
        gunicorn = (ROOT / "gunicorn.conf.py").read_text(encoding="utf-8")
        post_fork = gunicorn[gunicorn.index("def post_fork"):]
        self.assertIn("rag_index.warm()", post_fork)
        self.assertNotIn("publishing_worker", post_fork)
        self.assertNotIn("build_publishing_worker", post_fork)

        compose = (ROOT / "docker-compose.prod.yml").read_text(encoding="utf-8")
        self.assertIn("attachment-worker:", compose)
        self.assertIn('command: ["python", "-m", "tools.attachment_worker"]', compose)

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
        self.assertNotRegex(
            runbook,
            r'export [A-Z][A-Z0-9_]*="\$\(',
            "assignment failures must not be masked by the export builtin",
        )
        self.assertNotIn('source "$KEYDION_ROOT/.env.prod"', runbook)
        self.assertNotIn("source /Keydion/.env.prod", runbook)
        self.assertNotIn('chown root:keydion "$KEYDION_ROOT/.env.prod"', runbook)
        self.assertNotIn('chmod 0640 "$KEYDION_ROOT/.env.prod"', runbook)
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
        self.assertGreaterEqual(lower.count("tar -tf"), 3)
        self.assertIn("venv.tar", lower)
        self.assertIn(
            'mkdir --mode=0700 -- "$keydion_backup_dir"',
            lower,
        )
        self.assertNotIn(
            'install -d -m 0700 "$keydion_backup_dir"',
            lower,
        )
        self.assertIn("never reuse a backup id or backup directory", lower)
        self.assertRegex(
            lower,
            r"run the rollback section in\s+full before repairing",
        )
        self.assertIn("start again at section 1 in a new root bash", lower)
        self.assertRegex(lower, r"new\s+`?keydion_backup_id")
        self.assertIn('-m pip install', lower)
        self.assertIn(
            '--requirement "$keydion_root/requirements.txt"',
            lower,
        )
        self.assertIn('-m pip check', lower)
        self.assertLess(
            lower.index(
                'git -c "$keydion_root" checkout --detach '
                '"$keydion_new_release"'
            ),
            lower.index('-m pip install'),
        )
        self.assertLess(
            lower.index('-m pip install'),
            lower.index("tools/preflight_publishing_migration.py"),
        )
        self.assertIn("@@global.server_uuid", lower)
        self.assertIn("database-identity.txt", lower)
        self.assertIn("keydion_application_db_identity", lower)
        self.assertIn("keydion_backup_db_identity", lower)
        self.assertIn("keydion_job_lease_seconds", lower)
        self.assertIn(
            'test "$keydion_job_lease_seconds" -le 1800',
            lower,
        )
        self.assertIn("must not exceed 1800 seconds", lower)
        before_stop = lower[:lower.index("## 2. stop the worker and web service")]
        self.assertNotIn("--untracked-files=no", lower)
        self.assertIn("--untracked-files=all", before_stop)
        self.assertGreaterEqual(lower.count("keydion_git_status"), 4)
        self.assertNotRegex(
            lower,
            r'test -z "\$\([^\n]*git -c',
            "git failures must not look like an empty clean status",
        )
        self.assertIn(
            '"$keydion_root/.venv/bin/python" -m pip check',
            before_stop,
        )
        self.assertIn("assert_no_systemd_dropins", lower)
        self.assertGreaterEqual(lower.count("dropinpaths"), 2)
        self.assertIn("restore rehearsal", lower)
        self.assertIn("keydion_restore_probe_database", lower)
        self.assertIn("--hex-blob", lower)
        self.assertIn("database-defaults.txt", lower)
        self.assertIn("database-source-metrics.txt", lower)
        self.assertIn("database-source-vectors.sha256", lower)
        self.assertIn("restore-rehearsal-metrics.txt", lower)
        self.assertIn("restore-rehearsal-vectors.sha256", lower)
        self.assertIn("octet_length(embedding_vec)", lower)
        self.assertIn("hex(embedding_vec)", lower)
        self.assertIn("keydion_database_character_set", lower)
        self.assertIn("keydion_database_collation", lower)
        self.assertIn("keydion_backup_required_bytes", lower)
        self.assertIn("keydion_backup_available_bytes", lower)
        self.assertIn("keydion_available_inodes", lower)
        self.assertIn("assert_rename_tree_not_mounted", lower)
        for tree in (
            '"$keydion_root/.venv"',
            '"$keydion_papers_dir"',
            '"$keydion_pending_dir"',
        ):
            self.assertIn(f"assert_rename_tree_not_mounted {tree}", lower)
        self.assertIn(
            'test "$(realpath "$keydion_root/.venv")" '
            '= "$keydion_root/.venv"',
            lower,
        )
        self.assertIn("systemd/unit-state.tsv", lower)
        self.assertIn("read_unit_enabled_state", lower)
        self.assertIn("read_unit_active_state", lower)
        stop_section = lower[
            lower.index("## 2. stop the worker and web service"):
            lower.index("## 3. take coordinated database")
        ]
        self.assertIn("systemctl disable keydion-publishing-worker", stop_section)
        self.assertIn("systemctl disable keydion", stop_section)
        self.assertIn("disabled", stop_section)
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

        resume_heading = "### Resume after an interrupted upgrade"
        self.assertIn(resume_heading.casefold(), lower)
        resume = lower[
            lower.index(resume_heading.casefold()):lower.index("## 7. validate")
        ]
        self.assertGreaterEqual(runbook.count("read_dotenv_value()"), 2)
        for required in (
            "sha256sum -c sha256sums",
            "old-release.txt",
            "new-release.txt",
            'test "$keydion_current_release" = "$keydion_new_release"',
            "read_unit_enabled_state",
            "read_unit_active_state",
            "disabled",
            "cmp --silent",
            "dropinpaths",
            "--untracked-files=all",
            "-m pip check",
            "keydion_job_lease_seconds",
            'stat -c \'%a\' "$keydion_mysql_defaults"',
            "database-identity.txt",
            "keydion_application_db_identity",
            "keydion_backup_db_identity",
            "tools/preflight_publishing_migration.py",
            "paperquery_publishing_maintenance=1",
            "alembic upgrade head",
        ):
            self.assertIn(required, resume)
        self.assertLess(
            resume.index('cd "$keydion_root"'),
            resume.index("-m tools.verify_alembic_state --code-only"),
        )
        self.assertIn("information_schema.tables", resume)
        self.assertGreaterEqual(lower.count("set -o noclobber"), 2)
        self.assertGreaterEqual(lower.count("tee -a \"$keydion_upgrade_log\""), 2)
        self.assertNotIn(
            'tee "$keydion_backup_dir/alembic-upgrade.txt"',
            lower,
        )

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
            "tools.verify_alembic_state",
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
        self.assertIn("restore_tree_once()", rollback)
        for marker in (
            "venv-restore.started",
            "venv-restore.complete",
            "papers-restore.started",
            "papers-restore.complete",
            "pending-papers-restore.started",
            "pending-papers-restore.complete",
        ):
            self.assertIn(marker, rollback)
        destructive_rollback = rollback[
            :rollback.index("only after those checks and reconciliation succeed")
        ]
        self.assertNotIn(
            'test ! -e "$keydion_root/.venv.failed-',
            destructive_rollback,
        )
        self.assertIn("rerun this same rollback block", rollback)
        self.assertIn("keydion_backup_server_uuid", rollback)
        self.assertIn("without selecting the application schema", rollback)
        self.assertIn("post-snapshot writes", rollback)
        self.assertIn("discards all", rollback)
        self.assertIn("failed-current-database.sql.gz", rollback)
        self.assertIn("failed-current-database.sha256", rollback)
        self.assertIn("preserve_failed_database_once", rollback)
        self.assertIn("restore_recorded_unit_state", rollback)
        self.assertIn("restore_recorded_unit_activity", rollback)
        self.assertIn("systemd/unit-state.tsv", rollback)
        self.assertIn("archive_failed_tree_once", rollback)
        self.assertIn("failed-trees", rollback)
        self.assertRegex(rollback, r"before\s+restarting section 1")
        self.assertIn("archive_failed_trees", rollback)
        worker_activity = rollback.index(
            "restore_recorded_unit_activity "
            "keydion-publishing-worker.service"
        )
        web_activity = rollback.index(
            "restore_recorded_unit_activity keydion.service"
        )
        self.assertLess(worker_activity, web_activity)
        activation = rollback[rollback.index(
            "only after those checks and reconciliation succeed"
        ):rollback.index("### clear retained failed trees")]
        for required in (
            "sudo /bin/bash",
            "sha256sum -c sha256sums",
            "systemd/unit-state.tsv",
            "mapfile -t",
            "reconciled",
        ):
            self.assertIn(required, activation)
        self.assertNotIn("drop database", activation)
        absent_marker = rollback.index(
            "systemd/keydion-publishing-worker.service.absent"
        )
        guarded_disable = rollback.index(
            "if systemctl cat keydion-publishing-worker.service "
            ">/dev/null 2>&1; then",
            absent_marker,
        )
        disable = rollback.index(
            "sudo systemctl disable keydion-publishing-worker.service",
            guarded_disable,
        )
        remove = rollback.index(
            "sudo rm -f /etc/systemd/system/"
            "keydion-publishing-worker.service",
            disable,
        )
        self.assertLess(absent_marker, guarded_disable)
        self.assertLess(guarded_disable, disable)
        self.assertLess(disable, remove)
        for required in (
            "old release",
            "database backup",
            "papers.tar",
            "pending-papers.tar",
            "venv.tar",
            ".venv.failed-",
            "-m pip check",
            "together",
            "systemd/keydion.service",
            "systemd/keydion-publishing-worker.service",
            "systemctl daemon-reload",
        ):
            self.assertIn(required, rollback)

    def test_runbook_dotenv_parser_rejects_ambiguous_systemd_syntax(self):
        runbook = self._required_text(RUNBOOK)
        function_start = runbook.index("read_dotenv_value() {")
        function_end = runbook.index(
            "\n\nPAPERQUERY_DATABASE_URL=", function_start
        )
        parser_function = runbook[function_start:function_end]
        script = "\n".join(
            (
                "set -euo pipefail",
                'KEYDION_ROOT="$1"',
                parser_function,
                'read_dotenv_value PAPERQUERY_DATA_DIR 0',
            )
        )

        fixtures = {
            "leading whitespace": " PAPERQUERY_DATA_DIR=/srv/other\n",
            "whitespace before equals": "PAPERQUERY_DATA_DIR =/srv/other\n",
            "continuation": "UNRELATED=value\\\nPAPERQUERY_DATA_DIR=/srv/other\n",
            "compound quote": 'PAPERQUERY_DATA_DIR="/Keydion"/data\n',
            "duplicate": (
                "PAPERQUERY_DATA_DIR=/Keydion/data\n"
                "PAPERQUERY_DATA_DIR=/srv/other\n"
            ),
        }
        with tempfile.TemporaryDirectory() as temporary_root:
            env_file = Path(temporary_root) / ".env.prod"
            for label, contents in fixtures.items():
                with self.subTest(label=label):
                    env_file.write_text(contents, encoding="utf-8")
                    result = subprocess.run(
                        ["bash", "-c", script, "bash", temporary_root],
                        check=False,
                        capture_output=True,
                        text=True,
                    )
                    self.assertNotEqual(0, result.returncode, result.stdout)

            env_file.write_text(
                "PAPERQUERY_DATA_DIR=/Keydion/data\n", encoding="utf-8"
            )
            valid = subprocess.run(
                ["bash", "-c", script, "bash", temporary_root],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(0, valid.returncode, valid.stderr)
            self.assertEqual("/Keydion/data", valid.stdout)

    def test_runbook_revalidates_tracked_unit_fragment_provenance(self):
        runbook = self._required_text(RUNBOOK).casefold()
        candidate = runbook[
            runbook.index("only after that coordinated backup"):
            runbook.index("## 4. run the read-only preflight")
        ]
        resume = runbook[
            runbook.index("### resume after an interrupted upgrade"):
            runbook.index("## 7. validate the migrated state")
        ]
        rollback = runbook[runbook.index("## rollback"):]
        activation = rollback[
            rollback.index("only after those checks and reconciliation succeed"):
            rollback.index("### clear retained failed trees")
        ]
        for section in (resume, rollback, activation):
            self.assertIn("fragmentpath", section)
            self.assertIn("/etc/systemd/system/$unit", section)
            self.assertIn('test ! -l "$expected"', section)
        for unit in (
            "keydion.service",
            "keydion-publishing-worker.service",
        ):
            self.assertIn(f"assert_tracked_unit_fragment {unit}", candidate)
        self.assertGreaterEqual(
            rollback.count("assert_tracked_unit_fragment keydion.service"),
            2,
        )

    def test_runbook_keeps_old_checkout_tracked_and_index_clean(self):
        runbook = self._required_text(RUNBOOK).casefold()
        rollback = runbook[runbook.index("## rollback"):]
        checkout = rollback.index(
            'git -c "$keydion_root" checkout --detach \\\n'
            '  "$keydion_old_release"'
        )
        cleanliness = (
            'git -c "$keydion_root" diff --quiet --',
            'git -c "$keydion_root" diff --cached --quiet --',
        )
        for gate in cleanliness:
            self.assertIn(gate, rollback[:checkout])
            self.assertIn(gate, rollback[checkout:])

        activation = rollback[
            rollback.index("only after those checks and reconciliation succeed"):
            rollback.index("### clear retained failed trees")
        ]
        first_start = activation.index('sudo systemctl start "$unit"')
        for gate in cleanliness:
            self.assertIn(gate, activation[:first_start])

    def test_runbook_persists_original_unit_state_before_disabling(self):
        runbook = self._required_text(RUNBOOK).casefold()
        before_stop = runbook[:runbook.index("## 2. stop the worker and web service")]
        stop = runbook[
            runbook.index("## 2. stop the worker and web service"):
            runbook.index("## 3. take coordinated database")
        ]
        backup = runbook[
            runbook.index("## 3. take coordinated database"):
            runbook.index("## 4. run the read-only preflight")
        ]
        for required in (
            "paper-publishing-active",
            "active-boundary",
            "systemd/unit-state.tsv",
            "pre-snapshot.sha256",
            "old-release.txt",
            "new-release.txt",
        ):
            self.assertIn(required, before_stop)
        self.assertIn('sync -f "$keydion_backup_dir/active-boundary"', before_stop)
        self.assertIn(
            'ln "$keydion_backup_dir/active-boundary"',
            before_stop,
        )
        self.assertNotIn("> \"$keydion_backup_dir/systemd/unit-state.tsv\"", backup)
        self.assertIn("continue after an interruption before section 3", stop)
        self.assertIn("sha256sum -c pre-snapshot.sha256", stop)
        self.assertLess(
            before_stop.index("systemd/unit-state.tsv"),
            runbook.index("systemctl disable keydion-publishing-worker"),
        )

    def test_runbook_validates_authoritative_storage_and_sensitive_paths(self):
        runbook = self._required_text(RUNBOOK)
        lower = runbook.casefold()
        before_stop = lower[:lower.index("## 2. stop the worker and web service")]
        self.assertNotIn('chown root:keydion "$keydion_root/.env.prod"', before_stop)
        self.assertNotIn('chmod 0640 "$keydion_root/.env.prod"', before_stop)
        for required in (
            'test ! -l "$keydion_root/.env.prod"',
            'test "$(realpath "$keydion_root/.env.prod")" = "$keydion_root/.env.prod"',
            'stat -c \'%u:%g:%a\' "$keydion_root/.env.prod"',
            'test -d "$keydion_papers_dir"',
            'test ! -l "$keydion_papers_dir"',
            'test -d "$keydion_pending_dir"',
            'test ! -l "$keydion_pending_dir"',
            "keydion_expected_papers_source",
            "keydion_expected_pending_source",
            "keydion_papers_device",
            "keydion_pending_device",
        ):
            self.assertIn(required, before_stop)
        install_line = before_stop.index("sudo install -d")
        for path in (
            '"$keydion_root/resource_files"',
            '"$keydion_root/static/uploads"',
        ):
            self.assertIn(f"test ! -l {path}", before_stop[:install_line])
        install_command = before_stop[install_line:before_stop.index("sudo -u keydion test -r")]
        self.assertNotIn('"$keydion_papers_dir"', install_command)
        self.assertNotIn('"$keydion_pending_dir"', install_command)

    def test_capacity_helpers_explicitly_propagate_pipeline_failures(self):
        runbook = self._required_text(RUNBOOK).casefold()
        preflight = runbook[
            runbook.index("tree_bytes() {"):
            runbook.index("assert_restore_capacity() {")
        ]
        for helper in (
            "tree_bytes() {",
            "tree_inodes() {",
            "available_bytes() {",
            "available_inodes() {",
        ):
            start = preflight.index(helper)
            end = preflight.index("\n}\n", start) + 3
            body = preflight[start:end]
            self.assertIn("if !", body)
            self.assertIn("return 1", body)

    def test_standalone_activation_revalidates_recorded_venv(self):
        runbook = self._required_text(RUNBOOK).casefold()
        activation = runbook[
            runbook.index("only after those checks and reconciliation succeed"):
            runbook.index("### clear retained failed trees")
        ]
        self.assertIn('"$keydion_venv_path/bin/python" -m pip check', activation)
        pip_check = activation.index(
            '"$keydion_venv_path/bin/python" -m pip check'
        )
        for required in (
            "venv-path.txt",
            'test "$keydion_venv_path" = "$keydion_root/.venv"',
            'test -d "$keydion_venv_path"',
            'test ! -l "$keydion_venv_path"',
            'test "$(realpath "$keydion_venv_path")" = "$keydion_venv_path"',
        ):
            self.assertIn(required, activation[:pip_check])

    def test_standalone_activation_freshly_binds_both_database_clients(self):
        runbook = self._required_text(RUNBOOK).casefold()
        activation = runbook[
            runbook.index("only after those checks and reconciliation succeed"):
            runbook.index("### clear retained failed trees")
        ]
        first_activity = activation.index(
            "restore_recorded_unit_activity keydion-publishing-worker.service"
        )
        guard = activation.rfind(
            "assert_fresh_database_identity", 0, first_activity
        )
        self.assertGreaterEqual(guard, 0)
        for required in (
            "export keydion_database=keydion",
            "export keydion_mysql_defaults=/root/.my.cnf",
            "read_dotenv_value()",
            "paperquery_database_url=",
            "database-identity.txt",
            "@@global.server_uuid, database()",
            "create_engine",
            "mysql --defaults-extra-file",
            'test -f "$keydion_root/.env.prod"',
            'test ! -l "$keydion_root/.env.prod"',
            "root:keydion:640",
        ):
            self.assertIn(required, activation[:first_activity])
        between = activation[guard:first_activity]
        self.assertNotIn("systemctl start", between)

    def test_activation_refences_retries_and_publishes_terminal_evidence(self):
        runbook = self._required_text(RUNBOOK).casefold()
        activation = runbook[
            runbook.index("only after those checks and reconciliation succeed"):
            runbook.index("### clear retained failed trees")
        ]
        for marker in (
            "rollback-started",
            "rollback-restored.complete",
            "rollback-activated.complete",
        ):
            self.assertIn(marker, activation)
        env_check = activation.index('test -f "$keydion_root/.env.prod"')
        absent_branch = activation.index(
            'if ! test -e "$keydion_rollback_activated"'
        )
        early_stop = activation.index("systemctl stop", absent_branch)
        self.assertLess(absent_branch, early_stop)
        self.assertLess(early_stop, env_check)

        approval = activation.index("keydion_reconciliation_approval")
        first_activity = activation.index(
            "restore_recorded_unit_activity keydion-publishing-worker.service"
        )
        provenance = activation.rfind(
            "assert_recorded_storage_provenance", approval, first_activity
        )
        identity = activation.rfind(
            "assert_fresh_database_identity", approval, first_activity
        )
        self.assertGreater(provenance, approval)
        self.assertGreater(identity, approval)

        terminal = activation.index(
            'publish_marker_once "$keydion_rollback_activated"',
            first_activity,
        )
        self.assertGreater(terminal, first_activity)
        self.assertIn("assert_recorded_unit_final", activation[first_activity:terminal])
        self.assertIn("publish_marker_once()", activation)

    def test_failed_tree_archival_is_cross_device_resumable_and_verified(self):
        runbook = self._required_text(RUNBOOK).casefold()
        archival = runbook[runbook.index("### clear retained failed trees"):]
        self.assertNotIn('mv -- "$source" "$destination"', archival)
        self.assertNotIn("same-device", archival)
        for marker in (
            "archive-complete",
            "source-delete-complete",
            ".tar.partial",
            ".sha256.partial",
        ):
            self.assertIn(marker, archival)
        function = archival[
            archival.index("archive_failed_tree_once() {"):
            archival.index("archive_failed_tree_once venv")
        ]
        committed = function.rindex(
            'publish_marker_once "$committed" "$expected_marker"'
        )
        verify = function.rindex("sha256sum -c", 0, committed)
        delete = function.index(
            'rm -rf --one-file-system -- "$source"', committed
        )
        removed = function.index(
            'publish_marker_once "$removed" "$expected_marker"', delete
        )
        self.assertLess(verify, committed)
        self.assertLess(committed, delete)
        self.assertLess(delete, removed)
        self.assertIn("tree-sha256", archival)

    def test_failed_tree_removed_retry_rebinds_archive_to_source_identity(self):
        runbook = self._required_text(RUNBOOK).casefold()
        archival = runbook[
            runbook.index("### clear retained failed trees"):
            runbook.index("### close the rollback window")
        ]
        function = archival[
            archival.index("archive_failed_tree_once() {"):
            archival.index("archive_failed_tree_once venv")
        ]
        removed_branch = function[
            function.index('if test -e "$removed"'):
            function.index('if test -f "$committed"')
        ]
        committed_presence = removed_branch.index('test -f "$committed"')
        committed_repair = removed_branch.index(
            'publish_marker_once "$committed" "$expected_marker"'
        )
        removed_repair = removed_branch.index(
            'publish_marker_once "$removed" "$expected_marker"'
        )
        digest = removed_branch.index('sha256sum "$destination"')
        binding = removed_branch.index('= "$source_digest"', digest)
        integrity = removed_branch.index('tar -tf "$destination"', binding)
        branch_return = removed_branch.index("return 0", integrity)
        self.assertLess(committed_presence, committed_repair)
        self.assertLess(committed_repair, removed_repair)
        self.assertLess(digest, binding)
        self.assertLess(binding, integrity)
        self.assertLess(integrity, branch_return)

    def test_rollback_selector_survives_the_one_release_window(self):
        runbook = self._required_text(RUNBOOK).casefold()
        section9 = runbook[
            runbook.index("## 9. restart worker and web"):
            runbook.index("## rollback")
        ]
        rollback = runbook[runbook.index("## rollback"):]
        activation = rollback[
            rollback.index("only after those checks and reconciliation succeed"):
            rollback.index("### clear retained failed trees")
        ]
        self.assertNotIn(
            "rm -f -- /srv/keydion-backups/paper-publishing-active",
            section9,
        )
        self.assertNotIn(
            "rm -f -- /srv/keydion-backups/paper-publishing-active",
            activation,
        )
        self.assertIn("paper-publishing-active", rollback)
        self.assertIn("one verified release", rollback)
        close_window = rollback[rollback.index("### close the rollback window"):]
        self.assertIn("close_rollback_window", close_window)
        self.assertIn("sha256sum -c sha256sums", close_window)
        self.assertIn(
            "rm -f -- /srv/keydion-backups/paper-publishing-active",
            close_window,
        )

    def test_forward_success_terminal_requires_live_and_boundary_validation(self):
        runbook = self._required_text(RUNBOOK).casefold()
        self.assertIn("### finalize the validated forward release", runbook)
        start = runbook.index("### finalize the validated forward release")
        forward = runbook[start:runbook.index("## rollback", start)]
        for required in (
            "forward-success.complete",
            "publish_marker_once()",
            "load_active_boundary()",
            "/run/lock/keydion-paper-publishing.lock",
            "assert_fresh_database_identity",
            "assert_recorded_storage_provenance",
            "database-identity.txt",
            "keydion_manifest_sha",
            "systemctl is-enabled",
            "systemctl is-active",
            "curl --silent --show-error",
            "301",
            "canonical",
            "legacy",
            "-m tools.verify_alembic_state",
        ):
            self.assertIn(required, forward)
        terminal = forward.index(
            'publish_marker_once "$keydion_forward_success"'
        )
        self.assertLess(forward.rindex("curl --silent --show-error"), terminal)
        self.assertLess(
            forward.rindex("assert_fresh_database_identity"), terminal
        )
        self.assertLess(
            forward.rindex("assert_recorded_storage_provenance"), terminal
        )
        self.assertIn('test ! -e "$keydion_backup_dir/rollback-started"', forward)
        post_live = forward[forward.rindex("curl --silent --show-error"):terminal]
        for required in (
            "rev-parse --verify head",
            "diff --quiet --",
            "status --porcelain --untracked-files=all",
            "fragmentpath",
            "dropinpaths",
            "cmp --silent",
        ):
            self.assertIn(required, post_live)

    def test_close_window_requires_the_matching_terminal_chain_and_live_state(self):
        runbook = self._required_text(RUNBOOK).casefold()
        close = runbook[
            runbook.index("### close the rollback window"):
            runbook.index("## retention boundary")
        ]
        for required in (
            "forward-success.complete",
            "rollback-started",
            "rollback-restored.complete",
            "rollback-activated.complete",
            "rollback-archives.complete",
            "assert_fresh_database_identity",
            "assert_recorded_storage_provenance",
            "assert_recorded_unit_final",
            "tree-sha256",
            "venv-restore.complete",
            "papers-restore.complete",
            "pending-papers-restore.complete",
            "sha256sum -c",
        ):
            self.assertIn(required, close)
        branch = close.index('if test -e "$keydion_rollback_started"')
        rollback_head = close.index(
            'test "$keydion_current_release" = "$keydion_old_release"', branch
        )
        forward_branch = close.index("else", rollback_head)
        forward_head = close.index(
            'test "$keydion_current_release" = "$keydion_new_release"',
            forward_branch,
        )
        self.assertIn("keydion_forward_success_expected", close[forward_branch:])
        approval = close.index("keydion_close_approval")
        remove = close.index(
            "rm -f -- /srv/keydion-backups/paper-publishing-active"
        )
        self.assertLess(rollback_head, approval)
        self.assertLess(forward_head, approval)
        self.assertLess(approval, remove)
        self.assertGreater(close.rindex("assert_fresh_database_identity"), approval)
        self.assertGreater(
            close.rindex("assert_recorded_storage_provenance"), approval
        )
        self.assertLess(close.rindex("assert_fresh_database_identity"), remove)

    def test_every_standalone_entry_proves_the_inode_bound_selector(self):
        runbook = self._required_text(RUNBOOK).casefold()
        boundaries = (
            ("### continue after an interruption before section 3", "## 3."),
            ("### resume after an interrupted upgrade", "## 7."),
            ("### finalize the validated forward release", "## rollback"),
            ("## rollback", "only after those checks and reconciliation succeed"),
            (
                "only after those checks and reconciliation succeed",
                "### clear retained failed trees",
            ),
            ("### clear retained failed trees", "### close the rollback window"),
            ("### close the rollback window", "## retention boundary"),
        )
        for start, end in boundaries:
            with self.subTest(start=start):
                section = runbook[runbook.index(start):runbook.index(end, runbook.index(start))]
                self.assertIn("load_active_boundary()", section)
                self.assertIn("paper-publishing-active", section)
                self.assertIn("root:root:600", section)
                self.assertIn("stat -c '%d:%i'", section)
                self.assertIn("active-boundary", section)
                self.assertIn("sha256sum -c pre-snapshot.sha256", section)
                self.assertNotIn("read -r -p \"backup id", section)

        continuation = runbook[
            runbook.index("### continue after an interruption before section 3"):
            runbook.index("## 3. take coordinated database")
        ]
        self.assertIn(
            "sync -f /srv/keydion-backups/paper-publishing-active",
            continuation,
        )
        sync_selector = continuation.index(
            "sync -f /srv/keydion-backups/paper-publishing-active"
        )
        disable = continuation.index("systemctl disable")
        self.assertLess(sync_selector, disable)

    def test_every_mutating_entry_takes_the_process_lifetime_global_lock(self):
        runbook = self._required_text(RUNBOOK).casefold()
        entries = (
            ("## 1. verify the host", "## 2. stop the worker", "mkdir --mode=0700"),
            (
                "### continue after an interruption before section 3",
                "## 3. take coordinated database",
                "sync -f /srv/keydion-backups/paper-publishing-active",
            ),
            (
                "### resume after an interrupted upgrade",
                "## 7. validate the migrated state",
                ': > "$keydion_upgrade_log"',
            ),
            (
                "### finalize the validated forward release",
                "## rollback",
                'publish_marker_once "$keydion_forward_success"',
            ),
            ("## rollback", "only after those checks", "systemctl disable"),
            (
                "only after those checks and reconciliation succeed",
                "### clear retained failed trees",
                "restore_recorded_unit_activity keydion-publishing-worker.service",
            ),
            (
                "### clear retained failed trees",
                "### close the rollback window",
                'mkdir --mode=0700 -- "$keydion_failed_tree_dir"',
            ),
            (
                "### close the rollback window",
                "## retention boundary",
                "rm -f -- /srv/keydion-backups/paper-publishing-active",
            ),
        )
        for start, end, mutation in entries:
            with self.subTest(start=start):
                begin = runbook.index(start)
                section = runbook[begin:runbook.index(end, begin)]
                self.assertIn(
                    "/run/lock/keydion-paper-publishing.lock", section
                )
                lock = section.index("/run/lock/keydion-paper-publishing.lock")
                flock = section.index("flock --exclusive --nonblock", lock)
                self.assertLess(flock, section.index(mutation))

    def test_process_lifetime_lock_is_released_at_standalone_handoffs(self):
        runbook = self._required_text(RUNBOOK).casefold()
        entries = (
            (
                "## 9. restart worker and web",
                "### finalize the validated forward release",
                "systemctl start keydion",
            ),
            (
                "### finalize the validated forward release",
                "## rollback",
                'publish_marker_once "$keydion_forward_success"',
            ),
            (
                "## rollback",
                "only after those checks and reconciliation succeed",
                'publish_marker_once "$keydion_rollback_restored"',
            ),
            (
                "only after those checks and reconciliation succeed",
                "### clear retained failed trees",
                'publish_marker_once "$keydion_rollback_activated"',
            ),
            (
                "### clear retained failed trees",
                "### close the rollback window",
                'publish_marker_once "$keydion_rollback_archives"',
            ),
            (
                "### close the rollback window",
                "## retention boundary",
                "rm -f -- /srv/keydion-backups/paper-publishing-active",
            ),
        )
        for start, end, final_mutation in entries:
            with self.subTest(start=start):
                begin = runbook.index(start)
                section = runbook[begin:runbook.index(end, begin)]
                mutation = section.rindex(final_mutation)
                shell_exit = section.rindex("\nexit\n")
                self.assertLess(mutation, shell_exit)
        self.assertIn("releases the process-lifetime lock", runbook)

    def test_prebackup_continuation_closes_over_later_shared_state(self):
        runbook = self._required_text(RUNBOOK).casefold()
        continuation = runbook[
            runbook.index("### continue after an interruption before section 3"):
            runbook.index("## 3. take coordinated database")
        ]
        for required in (
            "assert_no_systemd_dropins()",
            "assert_tracked_unit_fragment()",
            "read_dotenv_value()",
            "paperquery_database_url=",
            "assert_fresh_database_identity()",
            "keydion_database_character_set",
            "keydion_mysql_capacity_evidence",
            "keydion_papers_dir",
            "keydion_pending_dir",
        ):
            self.assertIn(required, continuation)

    def _restore_probe_shell_functions(self) -> str:
        runbook = self._required_text(RUNBOOK)
        start_marker = "restore_probe_current_server_uuid() {"
        end_marker = "# End restore-probe ownership helpers."
        self.assertIn(start_marker, runbook)
        self.assertIn(end_marker, runbook)
        start = runbook.index(start_marker)
        end = runbook.index(end_marker, start)
        return runbook[start:end]

    def _run_restore_probe_scenario(
        self,
        functions: str,
        *,
        state: str,
        current_server_uuid: str,
        action: str = "prepare_restore_probe",
    ) -> tuple[subprocess.CompletedProcess[str], list[str], str]:
        recorded_server_uuid = "11111111-2222-3333-4444-555555555555"
        database = "keydion_restore_probe_0123456789abcdef0123456789abcdef"
        token = "a" * 64
        fake_mysql = r"""
mysql() {
  local database_arg="" sql="" state
  while test "$#" -gt 0; do
    case "$1" in
      --defaults-extra-file=*|--batch|--skip-column-names|--raw) shift ;;
      -e) sql="$2"; shift 2 ;;
      *) database_arg="$1"; shift ;;
    esac
  done
  state="$(cat "$FAKE_MYSQL_STATE")"
  case "$sql" in
    "SELECT @@GLOBAL.server_uuid")
      printf 'SERVER\n' >> "$FAKE_MYSQL_LOG"
      printf '%s\n' "$FAKE_CURRENT_SERVER_UUID"
      ;;
    *"FROM information_schema.SCHEMATA"*)
      [[ "$sql" == *"'$FAKE_EXPECTED_DATABASE'"* ]] || return 98
      printf 'EXISTS\n' >> "$FAKE_MYSQL_LOG"
      case "$state" in
        absent) printf '0\n' ;;
        *) printf '1\n' ;;
      esac
      ;;
    "SELECT ownership_token, server_uuid FROM "*)
      test "$database_arg" = "$FAKE_EXPECTED_DATABASE" || return 98
      [[ "$sql" == *'`_keydion_restore_probe_owner`'* ]] || return 98
      printf 'MARKER_READ\n' >> "$FAKE_MYSQL_LOG"
      case "$state" in
        exact)
          printf '%s\t%s\n' "$FAKE_EXPECTED_TOKEN" \
            "$FAKE_RECORDED_SERVER_UUID"
          ;;
        token-mismatch)
          printf '%064d\t%s\n' 0 "$FAKE_RECORDED_SERVER_UUID"
          ;;
        marker-server-mismatch)
          printf '%s\taaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\n' \
            "$FAKE_EXPECTED_TOKEN"
          ;;
        unmarked) return 1 ;;
        *) return 98 ;;
      esac
      ;;
    "DROP DATABASE "*)
      [[ "$sql" == *"\`$FAKE_EXPECTED_DATABASE\`"* ]] || return 98
      printf 'DROP\n' >> "$FAKE_MYSQL_LOG"
      printf 'absent\n' > "$FAKE_MYSQL_STATE"
      ;;
    "CREATE DATABASE "*)
      test "$state" = absent || return 1
      [[ "$sql" == *"\`$FAKE_EXPECTED_DATABASE\`"* ]] || return 98
      printf 'CREATE\n' >> "$FAKE_MYSQL_LOG"
      printf 'unmarked\n' > "$FAKE_MYSQL_STATE"
      ;;
    "CREATE TABLE "*)
      test "$state" = unmarked || return 1
      test "$database_arg" = "$FAKE_EXPECTED_DATABASE" || return 98
      [[ "$sql" == *'`_keydion_restore_probe_owner`'* ]] || return 98
      [[ "$sql" == *"'$FAKE_EXPECTED_TOKEN'"* ]] || return 98
      [[ "$sql" == *"'$FAKE_RECORDED_SERVER_UUID'"* ]] || return 98
      printf 'MARKER_WRITE\n' >> "$FAKE_MYSQL_LOG"
      printf 'exact\n' > "$FAKE_MYSQL_STATE"
      ;;
    *) return 97 ;;
  esac
}
"""
        with tempfile.TemporaryDirectory() as temporary_root:
            root = Path(temporary_root)
            state_path = root / "state"
            log_path = root / "mysql.log"
            state_path.write_text(f"{state}\n", encoding="utf-8")
            log_path.write_text("", encoding="utf-8")
            script = "\n".join(
                (
                    "set -euo pipefail",
                    'FAKE_MYSQL_STATE="$1"',
                    'FAKE_MYSQL_LOG="$2"',
                    f"FAKE_RECORDED_SERVER_UUID={recorded_server_uuid}",
                    f"FAKE_CURRENT_SERVER_UUID={current_server_uuid}",
                    f"FAKE_EXPECTED_DATABASE={database}",
                    f"FAKE_EXPECTED_TOKEN={token}",
                    "KEYDION_MYSQL_DEFAULTS=/fake/my.cnf",
                    f"KEYDION_RESTORE_PROBE_SERVER_UUID={recorded_server_uuid}",
                    f"KEYDION_RESTORE_PROBE_DATABASE={database}",
                    f"KEYDION_RESTORE_PROBE_TOKEN={token}",
                    "KEYDION_RESTORE_PROBE_MARKER_TABLE="
                    "_keydion_restore_probe_owner",
                    "KEYDION_DATABASE_CHARACTER_SET=utf8mb4",
                    "KEYDION_DATABASE_COLLATION=utf8mb4_unicode_ci",
                    fake_mysql,
                    functions,
                    action,
                )
            )
            result = subprocess.run(
                ["bash", "-c", script, "bash", str(state_path), str(log_path)],
                check=False,
                capture_output=True,
                text=True,
            )
            events = log_path.read_text(encoding="utf-8").splitlines()
            final_state = state_path.read_text(encoding="utf-8").strip()
            return result, events, final_state

    def test_restore_probe_metadata_is_random_private_and_checksummed(self):
        runbook = self._required_text(RUNBOOK).casefold()
        before_stop = runbook[
            runbook.index("## 1. verify the host"):
            runbook.index("## 2. stop the worker")
        ]
        backup = runbook[
            runbook.index("## 3. take coordinated database"):
            runbook.index("## 4. run the read-only preflight")
        ]
        for required in (
            "od -an -n16 -tx1 /dev/urandom",
            "od -an -n32 -tx1 /dev/urandom",
            "restore-probe.tsv",
            'root:root:600',
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
            '^keydion_restore_probe_[0-9a-f]{32}$',
            '^[0-9a-f]{64}$',
            'test "${#keydion_restore_probe_database}" -le 64',
        ):
            self.assertIn(required, before_stop)
        checksum = before_stop[
            before_stop.index("find old-release.txt"):
            before_stop.index("> pre-snapshot.sha256")
        ]
        self.assertIn("restore-probe.tsv", checksum)
        for required in (
            "mapfile -t keydion_restore_probe_lines",
            'test "${#keydion_restore_probe_lines[@]}" -eq 1',
            'cmp --silent - "$metadata"',
            "_keydion_restore_probe_owner",
            "information_schema.tables",
        ):
            self.assertIn(required, backup)
        marker_write = backup[
            backup.index('-e "create table \\`${keydion_restore_probe_marker_table}'):
            backup.index("\n", backup.index(
                '-e "create table \\`${keydion_restore_probe_marker_table}'
            ))
        ]
        self.assertIn(" as select ", marker_write)
        self.assertNotIn("; insert into", marker_write)
        normal = backup[:backup.index(
            "### guarded human recovery for an unmarked restore probe"
        )]
        helpers_end = normal.index("# end restore-probe ownership helpers.")
        prepare = normal.index("\nprepare_restore_probe\n", helpers_end)
        replay = normal.index("if gzip -dc", prepare)
        cleanup = normal.index("\ndrop_owned_restore_probe\n", replay)
        verdict = normal.index(
            'test "$keydion_restore_probe_succeeded" -eq 1', cleanup
        )
        self.assertLess(prepare, replay)
        self.assertLess(replay, cleanup)
        self.assertLess(cleanup, verdict)
        recovery = runbook[
            runbook.index("### guarded human recovery for an unmarked restore probe"):
            runbook.index("## 4. run the read-only preflight")
        ]
        for required in (
            "sha256sum -c pre-snapshot.sha256",
            "restore-probe.tsv",
            "incident-owner",
            "audit evidence",
            "information_schema.tables",
            "information_schema.routines",
            "routine_schema",
            "information_schema.events",
            "event_schema",
            "information_schema.triggers",
            "trigger_schema",
            "load_active_boundary",
        ):
            self.assertIn(required, recovery)
        for count_function in (
            "guarded_restore_probe_table_count",
            "guarded_restore_probe_routine_count",
            "guarded_restore_probe_event_count",
            "guarded_restore_probe_trigger_count",
        ):
            self.assertEqual(
                2,
                recovery.count(f'test "$({count_function})" = 0'),
            )
        self.assertNotIn("drop database if exists", recovery)

    def test_restore_probe_state_machine_executes_only_owned_transitions(self):
        functions = self._restore_probe_shell_functions()
        recorded_server_uuid = "11111111-2222-3333-4444-555555555555"

        result, events, final_state = self._run_restore_probe_scenario(
            functions,
            state="absent",
            current_server_uuid=recorded_server_uuid,
        )
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("exact", final_state)
        self.assertNotIn("DROP", events)
        create = events.index("CREATE")
        self.assertEqual("MARKER_WRITE", events[create + 1])

        result, events, final_state = self._run_restore_probe_scenario(
            functions,
            state="exact",
            current_server_uuid=recorded_server_uuid,
        )
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("exact", final_state)
        self.assertEqual(1, events.count("DROP"))
        drop = events.index("DROP")
        create = events.index("CREATE")
        self.assertEqual(["SERVER", "MARKER_READ"], events[drop - 2:drop])
        self.assertLess(drop, create)
        self.assertEqual("MARKER_WRITE", events[create + 1])

        refusal_scenarios = (
            ("unmarked", recorded_server_uuid),
            ("token-mismatch", recorded_server_uuid),
            ("marker-server-mismatch", recorded_server_uuid),
            ("exact", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
        )
        for state, current_server_uuid in refusal_scenarios:
            with self.subTest(state=state, current_server_uuid=current_server_uuid):
                result, events, final_state = self._run_restore_probe_scenario(
                    functions,
                    state=state,
                    current_server_uuid=current_server_uuid,
                )
                self.assertNotEqual(0, result.returncode, result.stdout)
                self.assertEqual(state, final_state)
                self.assertNotIn("DROP", events)
                self.assertNotIn("CREATE", events)
                self.assertNotIn("MARKER_WRITE", events)

        result, events, final_state = self._run_restore_probe_scenario(
            functions,
            state="exact",
            current_server_uuid=recorded_server_uuid,
            action="drop_owned_restore_probe",
        )
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("absent", final_state)
        self.assertEqual(["SERVER", "MARKER_READ", "DROP"], events)

    def test_prebackup_continuation_reloads_supported_unit_configuration(self):
        runbook = self._required_text(RUNBOOK).casefold()
        continuation = runbook[
            runbook.index("### continue after an interruption before section 3"):
            runbook.index("## 3. take coordinated database")
        ]
        for required in (
            "paperquery_data_dir=",
            "paperquery_upload_dir=",
            "paperquery_publishing_job_lease_seconds",
            '""|/keydion/data',
            '""|/keydion/papers',
            "1..1800",
            'local required="${2:-1}"',
        ):
            self.assertIn(required, continuation)
        parser = continuation[
            continuation.index("read_dotenv_value() {"):
            continuation.index("\n}\n", continuation.index("read_dotenv_value() {")) + 3
        ]
        self.assertIn('if test "$match_count" -eq 0', parser)
        self.assertIn('if test "$required" -eq 0', parser)

    def test_live_standalones_reload_full_config_after_human_pauses(self):
        runbook = self._required_text(RUNBOOK).casefold()
        sections = (
            (
                runbook[
                    runbook.index("### finalize the validated forward release"):
                    runbook.index("## rollback")
                ],
                "keydion_live_paper_id",
                'publish_marker_once "$keydion_forward_success"',
            ),
            (
                runbook[
                    runbook.index(
                        "only after those checks and reconciliation succeed"
                    ):
                    runbook.index("### clear retained failed trees")
                ],
                "keydion_reconciliation_approval",
                "restore_recorded_unit_activity keydion-publishing-worker.service",
            ),
            (
                runbook[
                    runbook.index("### close the rollback window"):
                    runbook.index("## retention boundary")
                ],
                "keydion_close_approval",
                "rm -f -- /srv/keydion-backups/paper-publishing-active",
            ),
        )
        for section, approval_name, mutation in sections:
            with self.subTest(approval=approval_name):
                for required in (
                    "reload_validated_runtime_config()",
                    "paperquery_database_url=",
                    "paperquery_data_dir=",
                    "paperquery_upload_dir=",
                    "paperquery_publishing_job_lease_seconds",
                    "must not exceed 1800 seconds",
                ):
                    self.assertIn(required, section)
                approval = section.index(approval_name)
                mutation_at = section.index(mutation, approval)
                reload_after_approval = section.index(
                    "reload_validated_runtime_config", approval
                )
                self.assertLess(reload_after_approval, mutation_at)
                identity = section.index(
                    "assert_fresh_database_identity", reload_after_approval
                )
                provenance = section.index(
                    "assert_recorded_storage_provenance", reload_after_approval
                )
                self.assertLess(identity, mutation_at)
                self.assertLess(provenance, mutation_at)

    def test_storage_provenance_is_fresh_before_restore_and_activation(self):
        runbook = self._required_text(RUNBOOK).casefold()
        rollback = runbook[runbook.index("## rollback"):]
        activation = rollback[
            rollback.index("only after those checks and reconciliation succeed"):
            rollback.index("### clear retained failed trees")
        ]
        first_restore = rollback.index("restore_tree_once venv")
        self.assertIn("assert_recorded_storage_provenance", rollback)
        storage_check = rollback.index("assert_recorded_storage_provenance")
        self.assertLess(storage_check, first_restore)
        database_replay = rollback.index(
            'gzip -dc "$keydion_backup_dir/database.sql.gz"'
        )
        papers_restore = rollback.index("\nrestore_tree_once papers\n")
        papers_guard = rollback.rfind(
            "assert_recorded_storage_provenance", database_replay, papers_restore
        )
        pending_restore = rollback.index(
            "\nrestore_tree_once pending-papers\n", papers_restore
        )
        pending_guard = rollback.rfind(
            "assert_recorded_storage_provenance", papers_restore, pending_restore
        )
        self.assertGreaterEqual(papers_guard, 0)
        self.assertGreaterEqual(pending_guard, 0)
        for section in (rollback[:first_restore], activation):
            for required in (
                "storage-sources.tsv",
                "findmnt --noheadings --raw --output source",
                "keydion_papers_mount_target",
                "keydion_pending_mount_target",
                "stat -c '%d'",
                "assert_recorded_storage_provenance",
            ):
                self.assertIn(required, section)
        activation_check = activation.index("assert_recorded_storage_provenance")
        activation_start = activation.index('sudo systemctl start "$unit"')
        self.assertLess(activation_check, activation_start)

    def test_upgrade_resume_revalidates_storage_before_preflight_and_upgrade(self):
        runbook = self._required_text(RUNBOOK).casefold()
        resume = runbook[
            runbook.index("### resume after an interrupted upgrade"):
            runbook.index("## 7. validate the migrated state")
        ]
        for required in (
            "assert_recorded_storage_provenance()",
            "storage-sources.tsv",
            "findmnt --noheadings --raw --output source",
            "keydion_papers_mount_target",
            "keydion_pending_mount_target",
            "stat -c '%d'",
            'sudo -u keydion test -r "$keydion_papers_dir"',
            'sudo -u keydion test -w "$keydion_pending_dir"',
        ):
            self.assertIn(required, resume)
        preflight = resume.index("tools/preflight_publishing_migration.py")
        first_check = resume.rfind(
            "assert_recorded_storage_provenance", 0, preflight
        )
        self.assertGreaterEqual(first_check, 0)
        upgrade = resume.index("-m alembic upgrade head")
        final_check = resume.rfind(
            "assert_recorded_storage_provenance", 0, upgrade
        )
        identity = resume.rfind("assert_fresh_database_identity", 0, upgrade)
        self.assertGreater(final_check, preflight)
        self.assertGreater(identity, preflight)

    def test_crash_markers_are_durable_before_and_after_tree_renames(self):
        runbook = self._required_text(RUNBOOK).casefold()
        before_stop = runbook[:runbook.index("## 2. stop the worker")]
        publish = before_stop.index(
            'ln "$keydion_backup_dir/active-boundary"'
        )
        self.assertIn(
            "sync -f /srv/keydion-backups/paper-publishing-active",
            before_stop[publish:],
        )
        link_sync = before_stop.index(
            "sync -f /srv/keydion-backups/paper-publishing-active",
            publish,
        )
        dir_sync = before_stop.index("sync -f /srv/keydion-backups\n", link_sync)
        disable = runbook.index("systemctl disable keydion-publishing-worker")
        self.assertLess(publish, link_sync)
        self.assertLess(link_sync, dir_sync)
        self.assertLess(dir_sync, disable)

        rollback = runbook[runbook.index("## rollback"):]
        function = rollback[
            rollback.index("restore_tree_once() {"):
            rollback.index("restore_tree_once venv")
        ]
        started_publish = function.index(
            'publish_marker_once "$started" "$expected_marker"'
        )
        first_rename = function.index('mv -- "$current" "$failed"')
        failed_parent_sync = function.index('sync -f "$failed_parent"', first_rename)
        restore_rename = function.index('mv -- "$stage/$entry" "$current"')
        current_parent_sync = function.index('sync -f "$current_parent"', restore_rename)
        complete_publish = function.index(
            'publish_marker_once "$complete" "$expected_marker"',
            current_parent_sync,
        )
        self.assertLess(started_publish, first_rename)
        self.assertLess(first_rename, failed_parent_sync)
        self.assertLess(restore_rename, current_parent_sync)
        self.assertLess(current_parent_sync, complete_publish)

        for required in (
            "archive_sha",
            "expected_marker",
            "complete without started",
            "failed tree without started",
            "missing current and stage",
            "tar --compare",
        ):
            self.assertIn(required, function)
        self.assertIn(
            'publish_marker_once "$started" "$expected_marker"', function
        )
        self.assertIn(
            'publish_marker_once "$complete" "$expected_marker"', function
        )
        self.assertNotIn('test "$(cat "$started")"', function)
        self.assertNotIn('test "$(cat "$complete")"', function)
        compare = function.index("tar --compare")
        complete_publish = function.index(
            'publish_marker_once "$complete" "$expected_marker"',
            compare,
        )
        self.assertLess(compare, complete_publish)

    def test_recovery_markers_publish_complete_payloads_atomically(self):
        runbook = self._required_text(RUNBOOK).casefold()
        forward = runbook[
            runbook.index("### finalize the validated forward release"):
            runbook.index("## rollback")
        ]
        rollback = runbook[
            runbook.index("## rollback"):
            runbook.index("only after those checks and reconciliation succeed")
        ]
        activation = runbook[
            runbook.index("only after those checks and reconciliation succeed"):
            runbook.index("### clear retained failed trees")
        ]
        archival = runbook[
            runbook.index("### clear retained failed trees"):
            runbook.index("### close the rollback window")
        ]
        for section in (forward, rollback, activation, archival):
            self.assertIn("publish_marker_once()", section)
            publisher_start = section.index("publish_marker_once() {")
            publisher_end = section.index("\n}\n", publisher_start) + 3
            publisher = section[publisher_start:publisher_end]
            for required in (
                "mktemp",
                'test -d "$parent"',
                'test ! -l "$parent"',
                'test ! -l "$target"',
                'test -f "$partial"',
                'printf \'%s\\n\' "$expected" > "$partial"',
                'sync -f "$partial"',
                'ln -- "$partial" "$target"',
                'printf \'%s\\n\' "$expected" | cmp --silent - "$target"',
                'sync -f "$target"',
                'sync -f "$parent"',
                'rm -f -- "$partial"',
            ):
                self.assertIn(required, publisher)
            self.assertIn("stale", publisher)
            self.assertNotIn('test "$(cat "$target")"', publisher)

        self.assertIn(
            'publish_marker_once "$keydion_forward_success"', forward
        )
        self.assertIn(
            'publish_marker_once "$keydion_rollback_activated"', activation
        )

        for final_path in ("$started", "$complete"):
            self.assertNotIn(f'> {final_path}', rollback)
            self.assertIn(
                f'publish_marker_once "{final_path}" "$expected_marker"',
                rollback,
            )
        for final_path in ("$source_identity", "$committed", "$removed"):
            self.assertNotIn(f'> {final_path}', archival)
            self.assertIn(f'publish_marker_once "{final_path}"', archival)

    def test_existing_recovery_markers_are_resynced_before_acceptance(self):
        runbook = self._required_text(RUNBOOK).casefold()
        rollback = runbook[
            runbook.index("## rollback"):
            runbook.index("only after those checks and reconciliation succeed")
        ]
        restored_guard = rollback[
            rollback.index('if test -e "$keydion_rollback_restored"'):
            rollback.index("keydion_data_dir=", rollback.index(
                'if test -e "$keydion_rollback_restored"'
            ))
        ]
        self.assertIn(
            'publish_marker_once "$keydion_rollback_restored"', restored_guard
        )
        restore_function = rollback[
            rollback.index("restore_tree_once() {"):
            rollback.index("restore_tree_once venv")
        ]
        complete_branch = restore_function[
            restore_function.index('if test -e "$complete"'):
            restore_function.index('if test -e "$started"')
        ]
        self.assertIn('publish_marker_once "$started"', complete_branch)
        self.assertIn('publish_marker_once "$complete"', complete_branch)

        activation = runbook[
            runbook.index("only after those checks and reconciliation succeed"):
            runbook.index("### clear retained failed trees")
        ]
        required_terminals_start = activation.index(
            "for keydion_required_terminal in"
        )
        required_terminals = activation[
            required_terminals_start:
            activation.index("done", required_terminals_start) + len("done")
        ]
        required_presence = required_terminals.index(
            'test -f "$keydion_required_terminal_path"'
        )
        required_exactness = required_terminals.index(
            'cmp --silent - "$keydion_required_terminal_path"'
        )
        required_repair = required_terminals.index(
            'publish_marker_once "$keydion_required_terminal_path"'
        )
        self.assertLess(required_presence, required_exactness)
        self.assertLess(required_exactness, required_repair)
        existing_activation = activation[
            activation.index('else\n  test -f "$keydion_rollback_activated"'):
            activation.index('test -f "$keydion_root/.env.prod"')
        ]
        self.assertIn(
            'publish_marker_once "$keydion_rollback_activated"',
            existing_activation,
        )

        close = runbook[
            runbook.index("### close the rollback window"):
            runbook.index("## retention boundary")
        ]
        exact_helper = close[
            close.index("assert_exact_terminal() {"):
            close.index("\n}\n", close.index("assert_exact_terminal() {")) + 3
        ]
        self.assertIn('sync -f "$path"', exact_helper)
        self.assertIn('sync -f "$(dirname "$path")"', exact_helper)

    def test_rollback_checkout_allows_only_boundary_failed_trees(self):
        runbook = self._required_text(RUNBOOK).casefold()
        rollback = runbook[runbook.index("## rollback"):]
        activation = rollback[
            rollback.index("only after those checks and reconciliation succeed"):
            rollback.index("### clear retained failed trees")
        ]
        for section in (rollback, activation):
            self.assertIn("assert_boundary_checkout_safe()", section)
            self.assertIn("--porcelain=v1 -z", section)
            self.assertIn("mktemp", section)
            self.assertIn('read -r -d \'\' entry', section)
            for prefix in (
                '.venv.failed-${keydion_backup_id}',
                'papers.failed-${keydion_backup_id}',
                'data/pending_papers.failed-${keydion_backup_id}',
            ):
                self.assertIn(prefix, section)
        checkout = rollback.index('checkout --detach \\\n  "$keydion_old_release"')
        self.assertGreaterEqual(
            rollback[:checkout].count("assert_boundary_checkout_safe"),
            2,
        )
        self.assertIn("assert_boundary_checkout_safe", rollback[checkout:])

    def test_failed_tree_retention_binds_original_source_identity(self):
        runbook = self._required_text(RUNBOOK).casefold()
        archival = runbook[runbook.index("### clear retained failed trees"):]
        for required in (
            "source-identity",
            "source_device_inode",
            "source_digest",
            "committed",
            "removed",
        ):
            self.assertIn(required, archival)
        committed = archival.index(
            'publish_marker_once "$committed" "$expected_marker"'
        )
        deletion = archival.index(
            'rm -rf --one-file-system -- "$source"', committed
        )
        removed = archival.index(
            'publish_marker_once "$removed" "$expected_marker"', deletion
        )
        self.assertLess(committed, deletion)
        self.assertLess(deletion, removed)
        self.assertIn(
            'test "$(stat -c \'%d:%i\' "$source")" = "$source_device_inode"',
            archival,
        )
        close = archival[archival.index("### close the rollback window"):]
        self.assertIn(
            'assert_exact_terminal "$source_identity"',
            close,
        )

    def test_failed_tree_archival_publishes_bound_all_complete_terminal(self):
        runbook = self._required_text(RUNBOOK).casefold()
        archival = runbook[
            runbook.index("### clear retained failed trees"):
            runbook.index("### close the rollback window")
        ]
        for marker in (
            "rollback-started",
            "rollback-restored.complete",
            "rollback-activated.complete",
            "rollback-archives.complete",
        ):
            self.assertIn(marker, archival)
        first_archive = archival.index("archive_failed_tree_once venv")
        for required in (
            "keydion_rollback_started_expected",
            "keydion_rollback_restored_expected",
            "keydion_rollback_activated_expected",
        ):
            self.assertLess(archival.index(required), first_archive)
        terminal = archival.index(
            'publish_marker_once "$keydion_rollback_archives"'
        )
        self.assertGreater(terminal, archival.index("sha256sum -c tree-sha256"))
        self.assertGreater(terminal, archival.index("test -z \"$keydion_git_status\""))
        self.assertIn("tree_sha256", archival[:terminal])
        self.assertIn("keydion_manifest_sha", archival[:terminal])

    def test_rollback_safety_dump_is_durable_before_database_recreation(self):
        runbook = self._required_text(RUNBOOK).casefold()
        rollback = runbook[runbook.index("## rollback"):]
        preserve = rollback[
            rollback.index("preserve_failed_database_once() {"):
            rollback.index("preserve_failed_database_once\n")
        ]
        for required in (
            'sync -f "$partial"',
            'sync -f "$checksum_partial"',
            'sync -f "$dump"',
            'sync -f "$checksum"',
            'sync -f "$keydion_backup_dir"',
        ):
            self.assertIn(required, preserve)
        dump_sync = preserve.index('sync -f "$partial"')
        dump_commit = preserve.index('mv -- "$partial" "$dump"', dump_sync)
        final_dump_sync = preserve.index('sync -f "$dump"', dump_commit)
        checksum_sync = preserve.index('sync -f "$checksum_partial"')
        checksum_commit = preserve.index(
            'mv -- "$checksum_partial" "$checksum"', checksum_sync
        )
        final_checksum_sync = preserve.index('sync -f "$checksum"', checksum_commit)
        parent_sync = preserve.index(
            'sync -f "$keydion_backup_dir"', final_checksum_sync
        )
        verify = preserve.index("sha256sum -c failed-current-database.sha256", parent_sync)
        self.assertLess(dump_sync, dump_commit)
        self.assertLess(dump_commit, final_dump_sync)
        self.assertLess(checksum_sync, checksum_commit)
        self.assertLess(checksum_commit, final_checksum_sync)
        self.assertLess(final_checksum_sync, parent_sync)
        self.assertLess(parent_sync, verify)

        preserve_call = rollback.index("preserve_failed_database_once\n")
        destructive_sql = rollback.index("drop database if exists", preserve_call)
        self.assertLess(preserve_call, destructive_sql)

    def test_destructive_rollback_publishes_started_and_restored_terminals(self):
        runbook = self._required_text(RUNBOOK).casefold()
        rollback = runbook[
            runbook.index("## rollback"):
            runbook.index("only after those checks and reconciliation succeed")
        ]
        for marker in (
            "rollback-started",
            "rollback-restored.complete",
        ):
            self.assertIn(marker, rollback)
        first_mutation = rollback.index("systemctl disable")
        started_publish = rollback.index(
            'publish_marker_once "$keydion_rollback_started"'
        )
        self.assertLess(started_publish, first_mutation)

        restored_publish = rollback.rindex(
            'publish_marker_once "$keydion_rollback_restored"'
        )
        for required_before in (
            "restore_tree_once venv",
            "restore_tree_once papers",
            "restore_tree_once pending-papers",
            "database-restored-metrics.txt",
            "database-restored-vectors.sha256",
            "assert_fresh_database_identity",
            "assert_recorded_storage_provenance",
            "restore_recorded_unit_state keydion.service",
        ):
            self.assertLess(rollback.rindex(required_before), restored_publish)
        self.assertIn("database-identity.txt", rollback[:restored_publish])
        self.assertIn("sha256sums", rollback[:restored_publish])

    def test_destructive_rollback_refuses_a_completed_terminal_rerun(self):
        runbook = self._required_text(RUNBOOK).casefold()
        rollback = runbook[
            runbook.index("## rollback"):
            runbook.index("only after those checks and reconciliation succeed")
        ]
        restored_guard = rollback.index(
            'if test -e "$keydion_rollback_restored"'
        )
        restored_symlink_guard = rollback.index(
            'test -l "$keydion_rollback_restored"', restored_guard
        )
        later_terminal_guard = rollback.index(
            "for keydion_impossible_later_terminal in"
        )
        first_started_publish = rollback.index(
            'publish_marker_once "$keydion_rollback_started"'
        )
        restored_branch = rollback[
            restored_guard:
            rollback.index("fi", restored_guard) + len("fi")
        ]
        self.assertIn('test -f "$keydion_rollback_started"', restored_branch)
        self.assertIn('test ! -l "$keydion_rollback_started"', restored_branch)
        self.assertIn(
            'cmp --silent - "$keydion_rollback_started"', restored_branch
        )
        self.assertIn(
            'cmp --silent - "$keydion_rollback_restored"', restored_branch
        )
        restored_validation = rollback.index(
            'publish_marker_once "$keydion_rollback_restored"',
            restored_guard,
        )
        restored_expected = rollback.index(
            '"$keydion_rollback_restored_expected"', restored_validation
        )
        refusal = rollback.index(
            "refusing destructive rollback rerun", restored_expected
        )
        first_service_mutation = rollback.index("systemctl disable")
        database_recreation = rollback.index("drop database if exists")
        self.assertLess(later_terminal_guard, first_started_publish)
        self.assertLess(restored_guard, first_started_publish)
        self.assertLess(restored_guard, restored_symlink_guard)
        self.assertLess(restored_guard, first_service_mutation)
        self.assertLess(refusal, first_service_mutation)
        self.assertLess(refusal, database_recreation)
        for later_terminal in (
            "rollback-activated.complete",
            "rollback-archives.complete",
        ):
            self.assertIn(later_terminal, rollback[:first_service_mutation])

    def test_fresh_database_identity_immediately_guards_mutations(self):
        runbook = self._required_text(RUNBOOK).casefold()
        backup = runbook[
            runbook.index("## 3. take coordinated database"):
            runbook.index("## 4. run the read-only preflight")
        ]
        stamp = runbook[
            runbook.index("## 5. stamp only"):
            runbook.index("## 6. upgrade or safely resume")
        ]
        upgrade = runbook[
            runbook.index("## 6. upgrade or safely resume"):
            runbook.index("### resume after an interrupted upgrade")
        ]
        resume = runbook[
            runbook.index("### resume after an interrupted upgrade"):
            runbook.index("## 7. validate the migrated state")
        ]
        guarded_commands = (
            (backup, "mysqldump"),
            (stamp, "-m alembic stamp"),
            (upgrade, "-m alembic upgrade head"),
            (resume, "-m alembic upgrade head"),
        )
        for section, mutation in guarded_commands:
            mutation_token_at = section.index(mutation)
            mutation_at = section.rfind("sudo ", 0, mutation_token_at)
            self.assertGreaterEqual(mutation_at, 0, mutation)
            guard_at = section.rfind("assert_fresh_database_identity", 0, mutation_at)
            self.assertGreaterEqual(guard_at, 0, mutation)
            between = section[guard_at:mutation_at]
            self.assertNotIn("mysql --defaults", between)
            self.assertNotIn(".venv/bin/python", between)
        for standalone in (
            runbook[
                runbook.index("### continue after an interruption before section 3"):
                runbook.index("## 3. take coordinated database")
            ],
            resume,
        ):
            self.assertIn("read_dotenv_value()", standalone)
            self.assertIn("assert_fresh_database_identity()", standalone)
            self.assertNotIn("paperquery_database_url.txt", standalone)

    def test_readme_provisions_all_tracked_unit_write_paths(self):
        readme = self._required_text(ROOT / "README.md")
        production = readme[
            readme.index("## Production Deployment"):
            readme.index("### Updating the server")
        ]
        install_units = production.index(
            "sudo cp deploy/keydion.service /etc/systemd/system/keydion.service"
        )
        start_units = production.index(
            "sudo systemctl enable --now keydion-publishing-worker"
        )
        for path in (
            "/Keydion/papers",
            "/Keydion/data/pending_papers",
            "/Keydion/resource_files",
            "/Keydion/static/uploads",
            "/var/log/keydion",
        ):
            self.assertIn(path, production[:install_units])
        self.assertIn("install -d -o keydion -g keydion -m 0750", production)
        self.assertIn("stat -c '%U:%G:%a'", production)
        self.assertLess(install_units, start_units)
        self.assertIn("migration runbook", production.casefold())

    def test_section9_revalidates_full_live_boundary_before_first_service_start(self):
        runbook = self._required_text(RUNBOOK).casefold()
        section = runbook[
            runbook.index("## 9. restart worker and web"):
            runbook.index("### finalize the validated forward release")
        ]
        first_mutation = min(
            section.index("sudo systemctl enable keydion-publishing-worker"),
            section.index("sudo systemctl start keydion-publishing-worker"),
        )
        prestart = section[:first_mutation]
        for required in (
            'paperquery_database_url="$(read_dotenv_value paperquery_database_url)"',
            'paperquery_data_dir="$(read_dotenv_value paperquery_data_dir 0)"',
            'paperquery_upload_dir="$(read_dotenv_value paperquery_upload_dir 0)"',
            "paperquery_publishing_job_lease_seconds",
            'gunicorn_bind="$(read_dotenv_value gunicorn_bind)"',
            'test "$gunicorn_bind" = 127.0.0.1:5000',
            "assert_recorded_storage_provenance",
            "assert_fresh_database_identity",
            "rev-parse --verify head",
            '= "$keydion_new_release"',
            'git -c "$keydion_root" diff --quiet --',
            'git -c "$keydion_root" diff --cached --quiet --',
            "status --porcelain --untracked-files=all",
            'test -d "$keydion_root/.venv"',
            'test ! -l "$keydion_root/.venv"',
            'test -x "$keydion_root/.venv/bin/python"',
            '"$keydion_root/.venv/bin/python" -m pip check',
            "fragmentpath",
            "dropinpaths",
            "root:root:644",
            '"$keydion_new_release:deploy/$unit"',
            'git -c "$keydion_root" hash-object',
            'test "$installed_blob" = "$release_blob"',
            "tools.verify_alembic_state",
        ):
            self.assertIn(required, prestart)

    def test_failed_tree_archival_refuses_source_and_descendant_mounts_freshly(self):
        runbook = self._required_text(RUNBOOK).casefold()
        archival = runbook[
            runbook.index("### clear retained failed trees"):
            runbook.index("### close the rollback window")
        ]
        function = archival[
            archival.index("archive_failed_tree_once() {"):
            archival.index("archive_failed_tree_once venv")
        ]
        self.assertIn("assert_failed_tree_unmounted() {", function)
        helper = function[
            function.index("assert_failed_tree_unmounted() {"):
            function.index("\n}\n", function.index(
                "assert_failed_tree_unmounted() {"
            )) + 3
        ]
        for required in (
            'mount_targets="$(findmnt --raw --noheadings --output target)"',
            '"$tree"|"$tree"/*',
            "return 1",
        ):
            self.assertIn(required, helper)
        self.assertIn("if !", helper)
        self.assertNotIn("keydion_mount_targets", function)

        for label, expected in (
            ("venv", '.venv.failed-${keydion_backup_id}'),
            ("papers", 'papers.failed-${keydion_backup_id}'),
            ("pending-papers", 'data/pending_papers.failed-${keydion_backup_id}'),
        ):
            self.assertIn(f"{label})", function)
            self.assertIn(expected, function)
        self.assertIn('test "$source" = "$expected_source"', function)
        self.assertIn(
            'test "$(realpath -m -- "$source")" = "$expected_source"',
            function,
        )

        mount_call = 'assert_failed_tree_unmounted "$source"'
        first_hash = function.index('source_digest="$(tar --sort=name')
        archive_write = function.index('-cpf "$partial"')
        delete = function.index('rm -rf --one-file-system -- "$source"')
        self.assertGreaterEqual(function.rfind(mount_call, 0, first_hash), 0)
        self.assertGreater(
            function.rfind(mount_call, first_hash, archive_write),
            first_hash,
        )
        delete_guard = function.rfind(mount_call, archive_write, delete)
        self.assertGreater(delete_guard, archive_write)
        self.assertNotIn("tar ", function[delete_guard:delete])
        self.assertNotIn("sha256sum", function[delete_guard:delete])

    def test_original_web_unit_prefers_old_release_then_exact_legacy_allowlist(self):
        runbook = self._required_text(RUNBOOK).casefold()
        before_stop = runbook[:runbook.index("## 2. stop the worker and web")]
        state_capture = before_stop.index("keydion_web_enabled_state=")
        self.assertIn("resolve_web_unit_provenance() {", before_stop)
        provenance_start = before_stop.index(
            "resolve_web_unit_provenance() {"
        )
        provenance_end = before_stop.index("\n}\n", provenance_start) + 3
        provenance = before_stop[provenance_start:provenance_end]
        for required in (
            "fragmentpath",
            "dropinpaths",
            'test -f "$expected"',
            'test ! -l "$expected"',
            "root:root:644",
            'ls-tree --name-only "$keydion_old_release" -- deploy/keydion.service',
            "deploy/keydion.service)",
            'keydion_web_unit_origin=old-release',
            'keydion_web_unit_source_release="$keydion_old_release"',
            'keydion_web_unit_source_path=deploy/keydion.service',
            '"" )',
            'keydion_web_unit_origin=candidate-legacy-allowlist',
            'keydion_web_unit_source_release="$keydion_new_release"',
            'keydion_web_unit_source_path=tests/fixtures/deploy/keydion-legacy.service.fixture',
            '"${keydion_web_unit_source_release}:${keydion_web_unit_source_path}"',
            'git -c "$keydion_root" cat-file -e',
            'git -c "$keydion_root" hash-object',
            'test "$installed_blob" = "$release_blob"',
            'keydion_web_unit_sha256="$(sha256sum "$expected"',
        ):
            self.assertIn(required, provenance)
        old_branch = provenance[
            provenance.index("deploy/keydion.service)"):
            provenance.index('"" )')
        ]
        fallback = provenance[provenance.index('"" )'):]
        self.assertNotIn("keydion_new_release", old_branch)
        self.assertNotIn("keydion-legacy.service", old_branch)
        self.assertIn("keydion_new_release", fallback)
        self.assertIn("keydion-legacy.service", fallback)
        web_call = before_stop.index(
            "resolve_web_unit_provenance",
            provenance_end,
        )
        worker_call = before_stop.index(
            "assert_old_release_unit_provenance "
            "keydion-publishing-worker.service",
            web_call,
        )
        self.assertLess(provenance_end, web_call)
        self.assertLess(web_call, worker_call)
        self.assertLess(worker_call, state_capture)
        self.assertIn(
            '"$keydion_new_release:deploy/keydion.service"', before_stop
        )

    def test_legacy_allowlist_never_applies_to_worker_or_forward_install(self):
        runbook = self._required_text(RUNBOOK).casefold()
        before_stop = runbook[:runbook.index("## 2. stop the worker and web")]
        worker_helper_start = before_stop.index(
            "assert_old_release_unit_provenance() {"
        )
        worker_helper = before_stop[
            worker_helper_start:
            before_stop.index("\n}\n", worker_helper_start) + 3
        ]
        worker_presence_start = before_stop.index(
            "prove_optional_worker_presence() {"
        )
        worker_presence = before_stop[
            worker_presence_start:
            before_stop.index("\n}\n", worker_presence_start) + 3
        ]
        for section in (worker_helper, worker_presence):
            self.assertNotIn("keydion-legacy.service", section)
            self.assertNotIn("candidate-legacy-allowlist", section)

        forward = runbook[
            runbook.index("only after that coordinated backup"):
            runbook.index("## 4. run the read-only preflight")
        ]
        self.assertIn(
            '"$keydion_root/deploy/keydion.service"', forward
        )
        self.assertIn(
            '"$keydion_root/deploy/keydion-publishing-worker.service"',
            forward,
        )
        self.assertNotIn("keydion-legacy.service", forward)

    def test_unit_provenance_record_and_exact_snapshot_precede_service_fence(self):
        runbook = self._required_text(RUNBOOK).casefold()
        before_stop = runbook[:runbook.index("## 2. stop the worker and web")]
        self.assertIn("systemd/unit-provenance.tsv", before_stop)
        snapshot = before_stop.index(
            '"$keydion_backup_dir/systemd/keydion.service"'
        )
        provenance = before_stop.index("systemd/unit-provenance.tsv")
        checksum = before_stop.index("pre-snapshot.sha256")
        for required in (
            'keydion.service\\t%s\\t%s\\t%s\\t%s\\t%s',
            "keydion_web_unit_origin",
            "keydion_web_unit_source_release",
            "keydion_web_unit_source_path",
            "keydion_web_unit_git_blob",
            "keydion_web_unit_sha256",
            "keydion-publishing-worker.service",
        ):
            self.assertIn(required, before_stop)
        self.assertLess(snapshot, provenance)
        self.assertLess(provenance, checksum)
        recorded = before_stop[provenance:checksum]
        self.assertIn(
            'stat -c \'%u:%g:%a\' \\\n  "$keydion_backup_dir/systemd/unit-provenance.tsv"',
            recorded,
        )
        self.assertIn("root:root:600", recorded)
        checksum_manifest = before_stop[
            before_stop.rindex("find old-release.txt"):
            checksum
        ]
        self.assertIn("active-boundary systemd", checksum_manifest)

    def test_first_rollout_never_adopts_or_edits_installed_unit_before_backup(self):
        runbook = self._required_text(RUNBOOK).casefold()
        pre_backup = runbook[:runbook.index("## 3. take coordinated database")]
        for forbidden in (
            "install -m 0644",
            "systemctl daemon-reload",
            'checkout --detach "$keydion_new_release"',
        ):
            self.assertNotIn(forbidden, pre_backup)
        self.assertNotIn(
            "tests/fixtures/deploy/keydion-legacy.service.fixture /etc/systemd/system/keydion.service",
            runbook,
        )

    def test_continuation_revalidates_recorded_unit_sources_before_fencing(self):
        runbook = self._required_text(RUNBOOK).casefold()
        continuation = runbook[
            runbook.index("### continue after an interruption before section 3"):
            runbook.index("## 3. take coordinated database")
        ]
        for required in (
            "load_recorded_unit_provenance() {",
            "systemd/unit-provenance.tsv",
            "candidate-legacy-allowlist",
            "tests/fixtures/deploy/keydion-legacy.service.fixture",
            '"${source_release}:${source_path}"',
            'test "$current_blob" = "$recorded_blob"',
            'test "$current_sha256" = "$recorded_sha256"',
            "assert_recorded_unit_source keydion.service",
        ):
            self.assertIn(required, continuation)
        load_start = continuation.index("load_recorded_unit_provenance() {")
        load = continuation[
            load_start:continuation.index("\n}\n", load_start) + 3
        ]
        self.assertIn("root:root:600", load)
        source_check = continuation.index(
            "assert_recorded_unit_source keydion.service"
        )
        first_fence = min(
            continuation.index("sudo systemctl disable"),
            continuation.index("sudo systemctl stop"),
        )
        self.assertLess(source_check, first_fence)

    def test_rollback_validates_legacy_snapshot_from_recorded_git_source(self):
        runbook = self._required_text(RUNBOOK).casefold()
        rollback = runbook[
            runbook.index("## rollback"):
            runbook.index("only after those checks and reconciliation succeed")
        ]
        for required in (
            "load_recorded_unit_provenance() {",
            "systemd/unit-provenance.tsv",
            "candidate-legacy-allowlist",
            "tests/fixtures/deploy/keydion-legacy.service.fixture",
            '"${source_release}:${source_path}"',
            "assert_recorded_unit_source keydion.service",
        ):
            self.assertIn(required, rollback)
        load_start = rollback.index("load_recorded_unit_provenance() {")
        load = rollback[load_start:rollback.index("\n}\n", load_start) + 3]
        self.assertIn("root:root:600", load)
        checkout = rollback.index(
            'checkout --detach \\\n  "$keydion_old_release"'
        )
        source_check = rollback.index(
            "assert_recorded_unit_source keydion.service", checkout
        )
        restore = rollback.index(
            '"$keydion_backup_dir/systemd/keydion.service" \\\n    /etc/systemd/system/keydion.service',
            source_check,
        )
        live_check = rollback.index(
            "assert_recorded_unit_live keydion.service", restore
        )
        self.assertLess(checkout, source_check)
        self.assertLess(source_check, restore)
        self.assertLess(restore, live_check)

    def test_docs_require_reviewed_fixture_changes_before_the_window(self):
        docs = " ".join((
            self._required_text(ROOT / "README.md")
            + "\n"
            + self._required_text(RUNBOOK)
        ).casefold().split())
        for required in (
            "reviewed first-rollout artifact",
            "hard stop",
            "before the maintenance window",
            "never edit or copy installed host bytes",
        ):
            self.assertIn(required, docs)

    def test_optional_worker_absence_requires_unambiguous_systemd_proof(self):
        runbook = self._required_text(RUNBOOK).casefold()
        before_stop = runbook[:runbook.index("## 2. stop the worker and web")]
        stop = runbook[
            runbook.index("## 2. stop the worker and web"):
            runbook.index("## 3. take coordinated database")
        ]

        helper_name = "prove_optional_worker_presence() {"
        self.assertIn(helper_name, before_stop)
        helper_start = before_stop.index(helper_name)
        helper_end = before_stop.index("\n}\n", helper_start) + 3
        helper = before_stop[helper_start:helper_end]
        for required in (
            'if load_state="$(systemctl show --property=loadstate --value "$unit")"; then',
            "loaded)",
            "not-found)",
            'test ! -e "$expected"',
            'test ! -l "$expected"',
            'active_state="$(systemctl is-active "$unit" 2>/dev/null)"',
            'test "$active_state" = inactive',
            'test "$active_status" -ne 0',
            'enabled_state="$(systemctl is-enabled "$unit" 2>/dev/null)"',
            'test "$enabled_state" = not-found',
            'test "$enabled_status" -ne 0',
            "assert_old_release_unit_provenance "
            "keydion-publishing-worker.service",
            "keydion_worker_unit_present=1",
            "keydion_worker_unit_present=0",
            "*)",
            "return 1",
        ):
            self.assertIn(required, helper)
        self.assertNotIn("systemctl cat", helper)
        self.assertNotIn(
            "if systemctl cat keydion-publishing-worker.service", before_stop
        )

        loaded = helper[
            helper.index("loaded)"):
            helper.index("not-found)")
        ]
        self.assertLess(
            loaded.index(
                "assert_old_release_unit_provenance "
                "keydion-publishing-worker.service"
            ),
            loaded.index("keydion_worker_unit_present=1"),
        )
        absent = helper[helper.index("not-found)"):]
        absent_assignment = absent.index("keydion_worker_unit_present=0")
        for proof in (
            'test ! -e "$expected"',
            'test ! -l "$expected"',
            'test "$active_state" = inactive',
            'test "$active_status" -ne 0',
            'test "$enabled_state" = not-found',
            'test "$enabled_status" -ne 0',
        ):
            self.assertLess(absent.index(proof), absent_assignment)

        state_capture = before_stop.index("keydion_web_enabled_state=")
        first_proof = before_stop.index(
            "\nprove_optional_worker_presence\n", helper_end
        )
        final_proof = before_stop.rindex(
            "\nprove_optional_worker_presence\n", helper_end, state_capture
        )
        self.assertLess(first_proof, final_proof)
        self.assertLess(final_proof, state_capture)
        reprobe_guard = before_stop[final_proof:state_capture]
        self.assertIn(
            'test "$keydion_worker_unit_present" -eq', reprobe_guard
        )
        self.assertIn(
            '"$keydion_proven_worker_unit_present"', reprobe_guard
        )
        self.assertIn(
            'if test "$keydion_worker_unit_present" -eq 1; then', stop
        )

    def test_forward_finalizer_uses_real_alias_filename_column(self):
        runbook = self._required_text(RUNBOOK).casefold()
        finalizer = runbook[
            runbook.index("### finalize the validated forward release"):
            runbook.index("## rollback")
        ]
        self.assertIn(
            "select filename from paper_filename_aliases", finalizer
        )
        self.assertIn("order by filename limit 1", finalizer)
        self.assertNotIn(
            "select legacy_filename from paper_filename_aliases", finalizer
        )
        self.assertNotIn("order by legacy_filename", finalizer)

    def test_host_listener_contract_is_coherent_and_docker_socket_remains_explicit(self):
        env_example = self._required_text(ROOT / ".env.example")
        gunicorn = self._required_text(ROOT / "gunicorn.conf.py")
        run_prod = self._required_text(ROOT / "run_prod.sh")
        host_nginx = self._required_text(ROOT / "deploy/keydion.nginx.conf")
        docker_nginx = self._required_text(ROOT / "docker/nginx.conf")
        compose = self._required_text(ROOT / "docker-compose.prod.yml")
        self.assertEqual(1, env_example.count("GUNICORN_BIND=127.0.0.1:5000"))
        self.assertIn(
            'os.environ.get("GUNICORN_BIND", "127.0.0.1:5000")', gunicorn
        )
        self.assertIn(
            'GUNICORN_BIND="${GUNICORN_BIND:-127.0.0.1:5000}"', run_prod
        )
        self.assertIn("proxy_pass http://127.0.0.1:5000;", host_nginx)
        self.assertIn(
            'GUNICORN_BIND: "unix:/var/run/keydion/keydion.sock"', compose
        )
        self.assertIn("server unix:/var/run/keydion/keydion.sock;", docker_nginx)

        runbook = self._required_text(RUNBOOK).casefold()
        initial = runbook[:runbook.index("## 2. stop the worker and web")]
        section9 = runbook[
            runbook.index("## 9. restart worker and web"):
            runbook.index("### finalize the validated forward release")
        ]
        first_start = section9.index(
            "sudo systemctl enable keydion-publishing-worker"
        )
        finalizer = runbook[
            runbook.index("### finalize the validated forward release"):
            runbook.index("## rollback")
        ]
        for section in (initial, section9[:first_start], finalizer):
            self.assertIn(
                'gunicorn_bind="$(read_dotenv_value gunicorn_bind)"', section
            )
            self.assertIn(
                'test "$gunicorn_bind" = 127.0.0.1:5000', section
            )
        self.assertIn("http://127.0.0.1:5000/paper/", finalizer)
        self.assertIn("http://127.0.0.1:5000/preview/", finalizer)

    def test_readme_provisions_data_root_before_nested_storage(self):
        readme = self._required_text(ROOT / "README.md")
        production = readme[
            readme.index("## Production Deployment"):
            readme.index("### Updating the server")
        ]
        install_start = production.index("sudo install -d")
        install_end = production.index("for path in", install_start)
        install_command = production[install_start:install_end]
        verify_end = production.index("done", install_end)
        verify_loop = production[install_end:verify_end]
        exact_data = re.compile(r"(?<![/\w])/Keydion/data(?![/\w])")
        self.assertRegex(install_command, exact_data)
        self.assertRegex(verify_loop, exact_data)
        data_root = exact_data.search(install_command)
        self.assertIsNotNone(data_root)
        self.assertLess(
            data_root.start(), install_command.index("/Keydion/data/pending_papers")
        )

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
        self.assertIn("schema-neutral", readme.casefold())
        self.assertIn("paper-publishing-migration.md", readme)
        self.assertNotIn(
            "indexed automatically on upload", readme.casefold()
        )
        self.assertIn("durable", readme.casefold())
        self.assertIn("retry", readme.casefold())
        runbook = self._required_text(RUNBOOK).casefold()
        self.assertIn("update both tracked units together", runbook)
        self.assertIn("rerun the deployment contract", runbook)
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
        self.assertNotIn(
            "without changing whether the paper was published or deleted",
            context.casefold(),
        )
        self.assertIn(
            "may delay indexing or deletion cleanup",
            context.casefold(),
        )


if __name__ == "__main__":
    unittest.main()

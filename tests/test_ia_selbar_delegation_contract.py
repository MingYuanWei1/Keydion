"""Contract: the bulk-delete selection bar's click actions are actually wired.

renderSelBar() inserts #iaSelBar as a SIBLING of #iaGroups (via
groupsEl.parentNode.insertBefore / .appendChild). For the bar's
`sel-clear` / `sel-delete` buttons to ever fire, the click listener that
handles those actions must be bound to a node that contains the bar — i.e.
groupsEl.parentNode (or document), NOT bare groupsEl. A listener on groupsEl
never sees the bubbled clicks because the bar is its sibling, leaving the
selection feature stranded once a checkbox is ticked.
"""
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JS = (ROOT / "static" / "js" / "ia-subjects.js").read_text(encoding="utf-8")


class IaSelbarDelegationContractTest(unittest.TestCase):
    def test_selbar_inserted_relative_to_parent(self):
        # Establishes the premise the listener-binding test depends on:
        # the bar is placed against groupsEl.parentNode (the shared host),
        # not inside groupsEl. A `selbarHost` alias for groupsEl.parentNode
        # is accepted.
        self.assertRegex(
            JS,
            r"(groupsEl\.parentNode|selbarHost)\.(insertBefore|appendChild)"
            r"\(\s*bar",
            "renderSelBar no longer inserts the bar against the shared host",
        )
        # If the alias is used, it must in fact point at groupsEl.parentNode.
        if "selbarHost" in JS:
            self.assertRegex(
                JS,
                r"selbarHost\s*=\s*groupsEl\.parentNode",
                "selbarHost must alias groupsEl.parentNode",
            )

    def test_selbar_click_handler_not_bound_to_bare_groupsEl(self):
        # Find the click listener that handles the selection-bar actions.
        m = re.search(
            r"(\w+)\.addEventListener\(\s*['\"]click['\"][\s\S]*?"
            r"['\"]sel-(?:clear|delete)['\"]",
            JS,
        )
        self.assertIsNotNone(
            m, "no click listener found that handles sel-clear / sel-delete"
        )
        target = m.group(1)
        # The bar is a sibling of groupsEl, so a listener on bare groupsEl
        # never receives its clicks. It must be on an ancestor of the bar.
        self.assertNotEqual(
            target,
            "groupsEl",
            "sel-clear/sel-delete handler is bound to groupsEl, but #iaSelBar is "
            "a sibling of #iaGroups — bind to groupsEl.parentNode or document",
        )
        self.assertIn(
            target,
            {"document", "selbarHost"},
            "sel-clear/sel-delete handler must be bound to a node containing "
            "#iaSelBar (document, or a stored groupsEl.parentNode reference)",
        )


if __name__ == "__main__":
    unittest.main()

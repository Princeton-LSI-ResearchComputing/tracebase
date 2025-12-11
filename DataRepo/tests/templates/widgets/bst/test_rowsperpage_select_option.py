from typing import Dict

from django.template.loader import render_to_string

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.widgets.bst.rows_per_page_select import BSTRowsPerPageSelect


class BSTRowsPerPageSelectOptionTests(TracebaseTestCase):

    template_name = BSTRowsPerPageSelect.option_template_name

    def render_option_template(self, context):
        return render_to_string(self.template_name, context)

    def get_massaged_template_str(self, template_str: str):
        lines = template_str.splitlines()
        non_empty_lines = [f"{line}\n" for line in lines if line.strip()]
        return "".join(non_empty_lines)

    def assert_substrings(
        self, expected_substrings: list, template_str: str, context: dict
    ):
        for expected in expected_substrings:
            # assertIn has really ugly failure output.  assertTrue with msg set is better
            self.assertTrue(
                expected in template_str,
                msg=(
                    f"'{expected}' not found in:\n"
                    f"{self.get_massaged_template_str(template_str)}\n"
                    f"using context: {context}"
                ),
            )

    def test_opt_elem_name_default(self):
        context: Dict[str, dict] = BSTRowsPerPageSelect(60).get_context(
            "select", 10, None
        )
        for i, (_, group_choices, _) in enumerate(context["widget"]["optgroups"]):
            for j in range(len(group_choices)):
                opt_context = {"widget": context["widget"]["optgroups"][i][1][j]}
                template_str = self.render_option_template(opt_context)
                self.assert_substrings(
                    ['name="rows-per-page-option"'], template_str, opt_context
                )

    def test_opt_elem_name_custom(self):
        rpps = BSTRowsPerPageSelect(60, option_name="rpp")
        context: Dict[str, dict] = rpps.get_context("select", 10, None)
        for i, (_, grp_choices, _) in enumerate(context["widget"]["optgroups"]):
            for j in range(len(grp_choices)):
                opt_context = {"widget": context["widget"]["optgroups"][i][1][j]}
                template_str = self.render_option_template(opt_context)
                self.assert_substrings(['name="rpp"'], template_str, opt_context)

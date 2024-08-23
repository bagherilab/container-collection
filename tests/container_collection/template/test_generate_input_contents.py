import unittest

from jinja2.exceptions import UndefinedError

from container_collection.template.generate_input_contents import generate_input_contents


class TestGenerateInputContents(unittest.TestCase):
    def test_generate_input_conditions_single_condition_template_single_condition_dict(
        self,
    ):
        actual_rendered = generate_input_contents(
            "Lorem ipsum {{condition_a}}", [{"condition_a": "This is condition A!"}]
        )
        expected = ["Lorem ipsum This is condition A!"]
        self.assertCountEqual(expected, actual_rendered)

    def test_generate_input_conditions_double_condition_template_double_condition_dict(
        self,
    ):
        actual_rendered = generate_input_contents(
            "Lorem ipsum {{condition_a}} {{condition_b}}",
            [
                {
                    "condition_a": "This is condition A!",
                    "condition_b": "This is condition B!",
                }
            ],
        )
        expected = ["Lorem ipsum This is condition A! This is condition B!"]
        self.assertCountEqual(expected, actual_rendered)

    def test_generate_input_conditions_single_condition_template_two_single_condition_dict(
        self,
    ):
        actual_rendered = generate_input_contents(
            "Lorem ipsum {{condition_a}}",
            [
                {"condition_a": "This is condition A!"},
                {"condition_a": "This is condition B!"},
            ],
        )
        expected = [
            "Lorem ipsum This is condition A!",
            "Lorem ipsum This is condition B!",
        ]
        self.assertCountEqual(expected, actual_rendered)

    def test_generate_input_conditions_single_condition_template_double_condition_dict(
        self,
    ):
        actual_rendered = generate_input_contents(
            "Lorem ipsum {{condition_a}}",
            [
                {
                    "condition_a": "This is condition A!",
                    "condition_b": "This is condition B!",
                }
            ],
        )
        expected = ["Lorem ipsum This is condition A!"]
        self.assertCountEqual(expected, actual_rendered)

    def test_generate_input_conditions_double_condition_template_single_condition_dict(
        self,
    ):
        with self.assertRaises(UndefinedError):
            generate_input_contents(
                "Lorem ipsum {{condition_a}} {{condition_b}}",
                [{"condition_a": "This is condition A!"}],
            )

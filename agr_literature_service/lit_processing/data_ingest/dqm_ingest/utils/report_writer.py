from collections import defaultdict
from os import path, makedirs

REPORT_TYPE_FILE_NAME_POSTFIX = {
    "generic": "main",
    "title": "dqm_pubmed_differ_title",
    "differ": "dqm_pubmed_differ_other",
    "resource_unmatched": "resource_unmatched",
    "reference_no_resource": "reference_no_resource"
}


class ReportWriter:
    def __init__(self, mod_reports_dir, multimod_reports_file_path):
        self.mod_reports_dir = mod_reports_dir
        self.multimod_reports_file_path = multimod_reports_file_path
        self.report_file_handlers = defaultdict(dict)
        if not path.exists(mod_reports_dir):
            makedirs(mod_reports_dir)

    def get_report_file_name(self, mod, report_type):
        if report_type == "multi":
            return self.multimod_reports_file_path
        else:
            return self.mod_reports_dir + mod + "_" + REPORT_TYPE_FILE_NAME_POSTFIX[report_type]

    def write(self, mod: str, report_type: str, message: str):
        try:
            self.report_file_handlers[report_type][mod].write(message)
        except KeyError:
            self.report_file_handlers[report_type][mod] = open(
                self.get_report_file_name(mod=mod, report_type=report_type), "w")
            self.report_file_handlers[report_type][mod].write(message)

    def close(self):
        for mod_handlers_dict in self.report_file_handlers.values():
            for file_handler in mod_handlers_dict.values():
                file_handler.close()

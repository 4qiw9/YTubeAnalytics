import re
import logging
from src.analyzers.base_analyzer import BaseAnalyzer

class FindSectionAnalyzer(BaseAnalyzer):

    def __init__(self, analyze_list_csv, transcripts_dir, keywords):
        super().__init__(analyze_list_csv, transcripts_dir)
        self.keywords = keywords

    def analyze(self):
        # todo: implement me
        # given: keyword
        # iterate over files (from input list)
        # find occurs in transcript
        # then: collect occurs
        # export to csv
        return

from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re


class MRAbbreviationFinder(MRJob):

    OUTPUT_PROTOCOL = TextProtocol
    PATTERN_RE = re.compile(r'(?: |^)\w+\.[,;:?!]?(?: |$)\w?')
    ABBR_RE = re.compile(r"\w+\.")
    PROB_THRESH = 0.7

    def mapper(self, _, line):
        for match in self.PATTERN_RE.findall(line):
            if isinstance(match, str):
                for abbr in self.ABBR_RE.findall(match):
                    if isinstance(abbr, str):
                        yield abbr.lower(), match[-1].islower()

    def reducer(self, word, counters):
        total, lower = 0, 0
        for counter in counters:
            if counter:
                lower += 1
            total += 1
        if self.PROB_THRESH < lower/total and total > 25 :
            yield word, str((total, lower))


if __name__ == "__main__":
    MRAbbreviationFinder.run()
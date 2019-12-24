from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re

WORD_RE = re.compile(r"[\w']+")


class MRFrequentUpperWordFinder(MRJob):
    OUTPUT_PROTOCOL = TextProtocol
    
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), word[0].isupper()

    def reducer(self, word, counters):
        total, upper = 0, 0
        for counter in counters:
            if counter:
                upper += 1
            total += 1
        if upper > 0.5 * total and total > 10:
            yield word, str((total, upper))
        
        
if __name__ == '__main__':    
    MRFrequentUpperWordFinder.run()
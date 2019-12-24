from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re
from statistics import mean

WORD_RE = re.compile(r"[\w']+")


class MRMeanWordSizeFinder(MRJob):
    OUTPUT_PROTOCOL = TextProtocol
    
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield None, len(word)

    def reducer(self, _, sizes):
        yield 'Average word length', str(mean(sizes))
        
        
if __name__ == '__main__':    
    MRMeanWordSizeFinder.run()
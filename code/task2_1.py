from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re

WORD_RE = re.compile(r"[\w']+")


class MRMaxWordSizeFinder(MRJob):
    OUTPUT_PROTOCOL = TextProtocol
    
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield None, word

    def combiner(self, _, words):
        yield None, max(words, key=lambda word: len(word))

    def reducer(self, _, words):
        maxw = max(words, key=lambda word: len(word))
        yield maxw, str(len(maxw))
        
        
if __name__ == '__main__':    
    MRMaxWordSizeFinder.run()
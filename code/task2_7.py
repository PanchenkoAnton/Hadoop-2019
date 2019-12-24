from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re
import nltk
import pymorphy2

WORD_RE = re.compile(r"[\w']+")


class MRNamesFinder(MRJob):
    OUTPUT_PROTOCOL = TextProtocol
    
    PROB_THRESH =  0.7
    morph = pymorphy2.MorphAnalyzer()
    
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield None, word

    def reducer(self, _, words):
        s = set()
        for word in words:
            s.add(word)
        for word in s:
            for p in self.morph.parse(word):
                if 'Name' in p.tag and p.score >= self.PROB_THRESH:
                    yield word, str(p.score)
        
        
if __name__ == '__main__':    
    MRNamesFinder.run()
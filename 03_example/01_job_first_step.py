from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r'[\w]+')

class MRJobFirstStep(MRJob):

    def step(self):
        return [
            MRStep(mapper=self.mapper,
                   combine=self.combiner,
                   reducer=self.reducer)
        ]

    def mapper(sel, _, line):
        words = WORD_RE.findall(line)
        for word in words:
            yield word, 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
        MRJobFirstStep.run()
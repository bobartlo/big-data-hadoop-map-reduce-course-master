from mrjob.job import MRJob

class MRSimpleJob(MRJob):

    def mapper(selfself, _, line):
        yield 'line', 1
        yield 'words', len(line.split())
        yield 'chars', len(line)

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRSimpleJob.run()

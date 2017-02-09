import unittest
import ssconvert as xlconvert
mobj = xlconvert.CONVERT()

class TestConvert(unittest.TestCase):
    def test_fileconversion(self):
        res = mobj.xl_to_csv('test.xls', 'test.csv')
        self.assertEqual(res, 0)
        res = mobj.xl_to_csv('test2.xlsx', 'test2.csv')
        self.assertEqual(res, 0)

    def test_processfull(self):
        res = mobj.process_conversion('test.xls', 'test.csv')
        self.assertEqual(res, True)

    def test_row_col_count(self):
        res = mobj.match_lines(5, 5, '')
        self.assertEqual(res, True)
        res = mobj.match_lines(5, 6, '')
        self.assertEqual(res, False)

if __name__ == '__main__':
    unittest.main()

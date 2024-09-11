import unittest
from main import load_data, process_data, predict_financial_health

class TestMainFunctions(unittest.TestCase):
    def test_load_data(self):
        result = load_data()
        self.assertEqual(result, "Data loaded successfully")

    def test_process_data(self):
        data = "Sample data"
        result = process_data(data)
        self.assertEqual(result, "Data processed successfully")

    def test_predict_financial_health(self):
        processed_data = "Sample processed data"
        result = predict_financial_health(processed_data)
        self.assertEqual(result, "Financial health prediction complete")

if __name__ == '__main__':
    unittest.main()
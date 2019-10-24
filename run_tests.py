import logging
import unittest

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    logging.basicConfig(level=logging.CRITICAL)
    loader = unittest.TestLoader()
    suite = loader.discover('.', pattern = "test_*")
    unittest.TextTestRunner(verbosity=2).run(suite)
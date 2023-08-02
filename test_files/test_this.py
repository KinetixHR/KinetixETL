import logging

logging.basicConfig(filename='TEST_example.log', level=logging.DEBUG,format='%(levelname)s %(asctime)s %(message)s')
logging.info("Hello!")
logging.warning("HELLO")

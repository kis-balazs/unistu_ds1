import logging
from discovery_server_thread import find_primary


if __name__ == '__main__':
	logging.basicConfig(format='[%(asctime)s] [%(levelname)-05s] %(message)s', level=logging.DEBUG)
	find_primary()

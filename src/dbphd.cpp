#include "dbphd/dbphd.hpp"
#include <thread>
#include <chrono>

using namespace std::chrono_literals;

namespace dbphd {
	DBPHD::DBPHD() {
		std::this_thread::sleep_for(5s);
	}

	DBPHD::~DBPHD() {

	}
}

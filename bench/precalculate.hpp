#ifndef PRECALCULATE_HPP
#define PRECALCULATE_HPP

#include <vector>
#include <cstdint>

class Precalculator {
public:
	static std::vector<std::vector<int>> PrecalcValues;
	static volatile bool PreCalculated;
	static uint64_t Rows;
	static int Values;
	static int Columns;
	static bool PreCalculate();
};

#endif /* ifndef PRECALCULATE_HPP


 */

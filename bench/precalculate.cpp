#include "precalculate.hpp"

#include <cmath>
#include <cstdint>

using namespace std;
template <typename T>
constexpr T ipow(T num, unsigned int pow)
{
    return (pow >= sizeof(unsigned int)*8) ? 0 :
        pow == 0 ? 1 : num * ipow(num, pow-1);
}

std::vector<std::vector<int>> Precalculator::PrecalcValues;
int Precalculator::Values = 10;
int Precalculator::Columns = 6;
uint64_t Precalculator::Rows = 1000000;
volatile bool Precalculator::PreCalculated = false;
bool Precalculator::PreCalculate() {
	if(PreCalculated)
		return true;
	vector<int> vals;
	for(int i = 0; i < Values; ++i) 
		vals.push_back(i);
	PrecalcValues.resize(Rows);

	#pragma omp parallel for
	for(uint64_t row = 0;  row < Rows; ++row) {
		int iteration = row;
		vector<int> rowvals;
		for(int col = 0; col < Columns; ++col) {
			rowvals.push_back(vals[iteration%Values]);
			iteration /= Values;
		}
		PrecalcValues[row] = rowvals;
	}

	PreCalculated = true;
	return true;
}

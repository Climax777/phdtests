#include <random>
#include <iostream>
#include <cassert>


using namespace std;

int main(int argc, char *argv[])
{
    mt19937 gen(0);

    int iterations = 0;
    while(true) {
        iterations++;
            
        uniform_int_distribution<int> dist(0, 23);
        int res = dist(gen); 
        if((res >= 23) && (res <= 0)) {
            cout << "GROOOT POOOO! " <<  res << " " << iterations << endl;
            cout.flush();
        }
        assert(res <= 23 && res >= 0); 
    }
    
    return 0;
}

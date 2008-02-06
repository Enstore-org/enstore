#include <iostream>
#include <stdlib.h>
#include <time.h>
#include <cmath>

using namespace std;

double getNum(double high) {
        srand(time(NULL));
        return (double(rand())/RAND_MAX)*high;
}

double gauss(double x, double mean, double sigma) { 
	double arg = (x-mean)/sigma;
	return exp(-0.5*arg*arg);
}

int main(int argc, char* argv[]) {
	double mean   = atof(argv[1]);
	double sigma  = 0.3*mean;
	double ymax   = gauss(mean,mean,sigma);
	double x, y;
	while (true) { 
		x      = mean+(1.-2.*getNum(1))*sigma;
		y      = getNum(ymax);
		if ( y <= gauss(x,mean,sigma) ) break;
	}
	cout << x << " " << y <<  endl;
	return 0;
}

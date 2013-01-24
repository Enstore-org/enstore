#include <fstream>
#include <iostream>
#include <iomanip>
#include <string>
#include <stdlib.h>

using namespace std;

double getNum(double high) {
	return (double(rand())/RAND_MAX)*high;
}

static const long KB = 1024;
static const long MB = KB * KB;
static const long long GB = KB * MB;
static const long long TB = KB * GB;
static const int ichunk=MB/sizeof(double);

int main(int argc, char* argv[]) { 
	if (argc < 3) {
		cerr<<"Enter size and file name"<<endl;
		return 1;
        }
	float fsize     = atof(argv[1]);
	long long isize = (long long)fsize*MB;
	
	string name     = argv[2];

	long long  total_size = 0;
	ofstream file(name.c_str(), ios::out | ios::binary);
	long long pos;
	srand(time(NULL));
	while(total_size<isize) { 
		double*  buffer = new double[ichunk];
		for (int i=0;i<ichunk;i++) { 
			double a = getNum(1000.);
			buffer[i] = a;
		}
		pos=file.tellp();
		file.seekp(pos);
		file.write((char*)buffer, ichunk*sizeof(double));
		pos=file.tellp();
		delete [] buffer;
		total_size+=MB;
	}
	file.close();
	return 0;
}


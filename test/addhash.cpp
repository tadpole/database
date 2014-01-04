//addhash.cpp
//2014-01-05 01:35:13
//ktu

#include <iostream>
#include <cstdio>
using namespace std;
int main(int argc, char** argv)
{
	unsigned int s = sprintf("%X", argv[1]);
	printf("%X", s);
	return 0;
}
 

#include "cstdio"

unsigned int myhash(const char *str)
{
	unsigned int res = 1315423911;

	while (*str)
		res ^= ((res << 5) + (*str++) + (res >> 2));

	return (res & 0x7FFFFFFF);
}

int main(int argc, char** argv)
{
	printf("%X\n", myhash(argv[1]));
	return 0;
}

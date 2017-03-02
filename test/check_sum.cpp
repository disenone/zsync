#include <iostream>
#include <fstream>
#include <deque>
#include <stdint.h>

using namespace std;

#define CHECKSUM_M 65536

class RollingChecksum
{
public:
    RollingChecksum(const uint8_t* data=NULL, int len=0);
    void calcChecksum(const uint8_t* data, int len=0);
    void calcNext(uint8_t c);

    uint32_t ma;
    uint32_t mb;
    uint32_t ms;
    size_t msize;

    deque<char> mroll_queue;

};

RollingChecksum::RollingChecksum(const uint8_t* data, int len): ma(0), mb(0), ms(0)
{
    if(data)
        calcChecksum(data, len);
}


void RollingChecksum::calcChecksum(const uint8_t* data, int len)
{
    ma = 0;
    mb = 0;
    ms = 0;
    mroll_queue.resize(len);
    msize = len;   

    for(int i=0; i<len; ++i)
    {
        uint8_t c = data[i];
        ma += c;
        mb += (len - i) * c;
        mroll_queue[i] = c;
    }
    ma %= CHECKSUM_M;
    mb %= CHECKSUM_M;

    ms = ma + (mb << 16);
    
    cout << ma << " " << mb << " " << ms << " " << mroll_queue.size() << endl;
}

void RollingChecksum::calcNext(uint8_t c)
{
    ma = ma - mroll_queue[0] + c;
    mb = (mb + ma) % CHECKSUM_M;
    ms = (ma & 0xffff) + (mb << 16);
    mroll_queue.pop_front();
    mroll_queue.push_back(c);
}

int main()
{
    cout << int(1 == 2) << endl;

    const int chunk_size = 262144;
    ifstream f("sync_files/testdata");
    uint8_t * buf = new uint8_t[chunk_size];

    f.read(reinterpret_cast<char*>(buf), chunk_size);
    RollingChecksum roll(buf, chunk_size);
    uint8_t c;
    while(!f.eof())
    {
        f.read(reinterpret_cast<char*>(buf), chunk_size);
        int size = f.gcount();
        for(int i=0; i<size; ++i)
        {
            uint8_t c = buf[i];
            roll.ma = roll.ma - roll.mroll_queue[0] + c;
            roll.mb += roll.ma;
            roll.ms = (roll.ma & 0xffff) + (roll.mb << 16);
            roll.mroll_queue.pop_front();
            roll.mroll_queue.push_back(c);
        }
    }

    cout << roll.ma << " " << roll.mb << " " << roll.ms << " " << endl;

    delete buf;
}


module TrailDB;

import std.string : toStringz;
import std.conv;

import breadcrumbs;


immutable static BUFFER_SIZE = 1 << 18;
immutable static EVENT_DELIMITER = '|';
immutable static DIM_DELIMITER = '&';

class TrailDB {

    string _db_path;
    void* _db;
    uint _numCookies;
    uint _numDims;

    uint[BUFFER_SIZE] _buff;
    char[BUFFER_SIZE] res;

    this(string db_path)
    {
        _db_path = db_path;
        _db = bd_open(toStringz(db_path));
        _numCookies = bd_num_cookies(_db);
        _numDims = bd_num_fields(_db);
    }

    @property uint numCookies(){ return _numCookies; }
    @property uint numDimensions(){ return _numDims; }


    char[] get_trail_per_index(uint index)
    {
        uint len = bd_trail_decode(_db, index, _buff.ptr, BUFFER_SIZE, 0);
        assert((len % (_numDims + 2)) == 0);

        ulong size = 0;
        uint num_events = len / (_numDims + 2);
        for(uint e = 0; e < num_events; ++e)
        {
            auto ts_str = to!(char[])(_buff[e * (_numDims + 2)]); //timestamp
            res[size..size + ts_str.length] = ts_str;
            res[size + ts_str.length] = DIM_DELIMITER;
            size += ts_str.length + 1;
            for(uint j = 0; j < _numDims; ++j)
            {
                char[] val = decode_val(_buff[e * (_numDims + 2) + j + 1]);
                res[size..size + val.length] = val;
                res[size + val.length] = DIM_DELIMITER;
                size += val.length + 1;
            }
            res[size - 1] = EVENT_DELIMITER;
        }
        return res[0..size-1];
    }

    char[] decode_val(uint val)
    {
        char* raw_val = bd_lookup_value(_db, val);
        return to!(char[])(raw_val);
    }
}

module TrailDB;

import std.conv;
import std.datetime;
import std.path : buildPath;
import std.stdio;
import std.string : toStringz;

import breadcrumbs;


immutable static BUFFER_SIZE = 1 << 18;
immutable static EVENT_DELIMITER = '|';
immutable static DIM_DELIMITER = '&';

/* Convert a unix timestamp in seconds into a D DateTime object. */
DateTime unix_ts_to_datetime(uint timestamp) pure
{
    return DateTime(Date(1970, 1, 1)) + dur!"seconds"(timestamp);
}

class TrailDB {

    string _db_path;
    void* _db;
    uint _numCookies;
    uint _numDims;
    string[] _dimNames;
    uint _eventTypeInd;

    uint[BUFFER_SIZE] _buff;
    char[BUFFER_SIZE] res;

    this(string db_path)
    {
        _db_path = db_path;
        _db = bd_open(toStringz(db_path));
        _numCookies = bd_num_cookies(_db);
        _numDims = bd_num_fields(_db);
        read_dim_names();

        // store index of log line type dim.
        for(int i = 0; i < _dimNames.length; ++i)
            if(_dimNames[i] == "type")
            {
                _eventTypeInd = i;
                break;
            }
    }

    void close()
    {
        bd_close(_db);
    }

    @property uint numCookies(){ return _numCookies; }
    @property uint numDimensions(){ return _numDims; }
    @property string[] dimNames(){ return _dimNames; }


    char[] get_trail_per_index(uint index)
    {
        uint num_events = _rawDecode(index);

        ulong size = 0;
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

    // returns the number of events
    uint _rawDecode(uint index)
    {
        uint len = bd_trail_decode(_db, index, _buff.ptr, BUFFER_SIZE, 0);
        if((len % (_numDims + 2)) != 0)
            return 0;
        //assert((len % (_numDims + 2)) == 0); <- some cookies fail this test
        //potential bug in traildb. investigate.
        return len / (_numDims + 2);
    }

    DateTime[] get_trail_timestamps(uint index)
    {
        uint num_events = _rawDecode(index);
        DateTime[] res = new DateTime[num_events];
        for(int i = 0; i < num_events; ++i)
            res[i] = unix_ts_to_datetime(_buff[i * (_numDims + 2)]);
        return res;
    }

    uint num_events(uint index, string type = "")
    {
        if(type == "")
            return _rawDecode(index);

        uint tot_events = _rawDecode(index);

        uint evt_type_encoded = bd_lookup_token(
            _db, toStringz(type), _eventTypeInd);

        uint cnt = 0;
        for(int i = 0; i < tot_events; ++i)
        {
            uint val = _buff[i * (_numDims + 2) + _eventTypeInd + 1];
            if(val == evt_type_encoded)
                ++cnt;
        }

        return cnt;
    }

    char[] decode_val(uint val)
    {
        char* raw_val = bd_lookup_value(_db, val);
        return to!(char[])(raw_val);
    }

    void read_dim_names()
    {
        //_fieldNames = new string[];
        auto f = File(buildPath(_db_path, "fields"), "r");
        foreach(line; f.byLine())
        {
            _dimNames ~= to!string(line);
        }
        assert(_dimNames.length == _numDims);
    }
}

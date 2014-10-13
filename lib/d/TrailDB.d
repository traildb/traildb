module TrailDB;

import std.conv;
import std.datetime;
import std.path : buildPath;
import std.stdio;
import std.string : format, toStringz;

import traildb;


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
        _db = tdb_open(toStringz(db_path));
        _numCookies = tdb_num_cookies(_db);
        _numDims = tdb_num_fields(_db);
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
        tdb_close(_db);
    }

    @property uint numCookies(){ return _numCookies; }
    @property uint numDimensions(){ return _numDims; }
    @property string[] dimNames(){ return _dimNames; }
    @property bool hasCookieIndex() { return tdb_has_cookie_index(_db) == 1; }

    /* Returns the 16 bytes cookie ID at a given position in the DB. */
    void getCookieByInd(uint ind, ref ubyte[16] res)
    {
        auto raw_val = tdb_get_cookie(_db, ind);
        res[] = raw_val[0..16];
    }

    /* Returns the HEX string representing the cookie at a given index
       in the DB.
    */
    char[] getHEXCookieByInd(uint ind)
    {
        ubyte[16] cookie;
        this.getCookieByInd(ind, cookie);
        char[] cookiestr;
        foreach(u; cookie)
            cookiestr ~= format("%.2x",u);
        return cookiestr;
    }

    /* Returns index of a given cookie in the database. If the cookie is
       not present, returns -1.
    */
    long getIndForCookie(ref ubyte[16] cookie)
    {
        if(!this.hasCookieIndex)
            throw new Exception("This Database doesn't have cookie indexing." ~
                " Impossible to perform reverse lookup.");
        long val = tdb_get_cookie_id(_db, cookie);
        return val;
    }


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
        uint len = tdb_decode_trail(_db, index, _buff.ptr, BUFFER_SIZE, 0);
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

        uint evt_type_encoded = tdb_get_val(_db, _eventTypeInd, toStringz(type));

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
        char* raw_val = tdb_get_item_value(_db, val);
        return to!(char[])(raw_val);
    }

    void read_dim_names()
    {
        auto f = File(buildPath(_db_path, "fields"), "r");
        foreach(line; f.byLine())
        {
            _dimNames ~= to!string(line);
        }
        assert(_dimNames.length == _numDims);
    }
}

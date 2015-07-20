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

/* D Range representing trail of events */
struct Trail {
    TrailDB _tdb;
    uint idx = 0;
    ulong size;
    uint _num_fields;
    ulong _num_events;

    char[][] fields;
    uint[] _buff;

    this(TrailDB tdb_, uint[] buff_) // TrailDB has decode_val
    {
        _tdb = tdb_;
        _buff = buff_;
        _num_fields = tdb_._numDims;
        _num_events = _buff.length / (_num_fields + 1);

        fields = new char[][](_num_fields); // Note: revisit?
    }

    @property bool empty()
    {
        return (idx == _num_events);
    }

    @property char[][] front()
    {
        fields[] = []; // Clear old fields

        foreach(j ; 0.._num_fields)
        {
            char[] val;
            if(j == 0)
            {
                val = to!(char[])(_buff[idx * (_num_fields + 1)]);
            }
            else
            {
                val = _tdb.decode_val(_buff[idx * (_num_fields + 1) + j ]);
            }
            fields[j] = val;
        }

        return fields;
    }

    void popFront()
    {
        idx++;
    }
}

class TrailDB {

    string _db_path;
    void* _db;
    uint _numCookies;
    uint _numDims;
    string[] _dimNames;
    uint _eventTypeInd;

    uint[BUFFER_SIZE] _buff; // Stores a trail

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

    /* Returns trail of events (a D Range)*/
    Trail trail(uint cookie_index)
    {
        uint trail_size = tdb_decode_trail(_db, cookie_index, _buff.ptr, BUFFER_SIZE, 0);
        Trail trl = Trail(this, _buff[0..trail_size]);
        return trl;
    }

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

    // returns the number of events
    uint _rawDecode(uint index)
    {
        uint len = tdb_decode_trail(_db, index, _buff.ptr, BUFFER_SIZE, 0);
        assert((len % (_numDims + 1)) == 0);
        return len / (_numDims + 1);
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

        uint evt_type_encoded = tdb_get_item(_db, _eventTypeInd, toStringz(type));

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
        _dimNames ~= "timestamp";
        auto f = File(buildPath(_db_path, "fields"), "r");
        foreach(line; f.byLine())
        {
            _dimNames ~= to!string(line);
        }
        assert(_dimNames.length == _numDims);
    }
}

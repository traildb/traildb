module TrailDB;

import std.conv;
import std.datetime;
import std.path : buildPath;
import std.stdio;
import std.string : format, toStringz;

import traildb;


immutable static BUFFER_SIZE = 1 << 18;

/* Event in a TrailDB trail. Lazily computes field values. */
struct Event {
    void* _db; // Needed to get item value
    uint[] _buff; // Raw buffer of event

    this(void* db_, uint[] buff_)
    {
        _db = db_;
        _buff = buff_;
    }

    char[] opIndex(uint j)
    {
        if(j == 0) // Timestamp
        {
            return to!(char[])(_buff[0]);
        }
        else
        {
            return to!(char[])(tdb_get_item_value(_db, _buff[j]));
        }
    }
}


/* D Range representing trail of events */
struct Trail {
    void* _db;
    uint idx = 0;
    uint _num_fields;
    ulong _num_events;
    uint[] _buff; // Raw buffer of the trail

    this(void* db_, uint num_fields_, uint[] buff_)
    {
        _db = db_;
        _buff = buff_;
        _num_fields = num_fields_;
        _num_events = _buff.length / (_num_fields + 1);
    }

    @property bool empty()
    {
        return (idx == _num_events);
    }

    @property Event front()
    {
        Event evt = Event(_db, _buff[idx * (_num_fields + 1).. (idx + 1) * (_num_fields + 1)]);
        return evt;
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

    uint[BUFFER_SIZE] _buff; // Raw buffer of trail

    this(string db_path)
    {
        _db_path = db_path;
        _db = tdb_open(toStringz(db_path));
        _numCookies = tdb_num_cookies(_db);
        _numDims = tdb_num_fields(_db);
        read_dim_names();
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
        Trail trl = Trail(_db, _numDims, _buff[0..trail_size]);
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

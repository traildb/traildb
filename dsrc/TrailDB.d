module TrailDB;

import std.string : toStringz;
import std.conv;

import breadcrumbs;


class TrailDB {

    string _db_path;
    void* _db;

    this(string db_path)
    {
        _db_path = db_path;
        _db = bd_open(toStringz(db_path));
    }

    @property uint numCookies(){ return bd_num_cookies(_db); }
    @property uint numDimensions(){ return bd_num_fields(_db); }

}

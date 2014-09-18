import std.stdio;
import std.string;
import std.conv;

import breadcrumbs;

int main(string[] args)
{
    writeln("hello");
    auto DB = bd_open(toStringz(args[1]));
    writeln(bd_num_cookies(DB));

    auto num_f = bd_num_fields(DB);
    writeln("Number of fields: ", num_f);

    auto id = bd_lookup_cookie(DB, 0);
    writef("First cookie: %.2X", id[15]);

    immutable static uint buff_size = 200_000;
    uint[] BUFF = new uint[buff_size];
    auto r = bd_trail_decode(DB, 0, BUFF.ptr, buff_size, 0);
    // TS is first, then 9 fields, then trailing 0.
    writeln("Timestamp: ", BUFF[0]);

    for(int i = 1; i < num_f + 1; ++i)
    {
        char* raw_val = bd_lookup_value(DB, BUFF[i]);
        string val_str = to!string(raw_val);
        writeln("Field ", i, " value: ", val_str); // empty: no value
    }

    return 0;
}

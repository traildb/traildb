import std.stdio;

import TrailDB;

int main(string[] args)
{
    string path = args[1];

    auto DB = new TrailDB(path);
    writeln("Size of the DB: ", DB.numCookies);
    writeln("Number of dimensions: ", DB.numDimensions);
    writeln("Dimensions: ", DB.dimNames);
    writeln("DB has cookie index for reverse lookup: ", DB.hasCookieIndex);

    auto index = 230;
    writeln("--- Detail of cookie index ", index, " trail. ---");

    if(DB.hasCookieIndex)
        writeln("Cookie HEX: ", DB.getHEXCookieByInd(index));

    char[] trail = DB.get_trail_per_index(index);
    writeln("Full trail:\n", trail);

    auto ts = DB.get_trail_timestamps(index);
    writeln("Timestamps of events: ", ts);

    writeln("Number of events: ", DB.num_events(index));
    writeln("Number of pxl events: ", DB.num_events(index, "pxl"));
    writeln("Number of imp events: ", DB.num_events(index, "imp"));


    uint sum = 0;
    for(int i = 0; i <  DB.numCookies; ++i)
    {
        if( i % 1_000_000 == 0)
            writeln((cast(float)i)/1_000_000, "M cookies seen.");
        sum += DB.num_events(i, "pxl");
    }
    writeln("Average number of pxl fired per cookie: ",
        (cast(float)sum)/DB.numCookies);

    DB.close();

    return 0;
}

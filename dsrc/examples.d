import std.stdio;

import TrailDB;

int main(string[] args)
{
    string path = args[1];

    auto DB = new TrailDB(path);
    writeln("Size of the DB: ", DB.numCookies);
    writeln("Number of dimensions: ", DB.numDimensions);

    char[] trail = DB.get_trail_per_index(120);
    writeln("Trail of cookie 120:\n", trail);

    return 0;
}

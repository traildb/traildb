import std.stdio;

import TrailDB;

int main(string[] args)
{
    auto DB = new TrailDB(args[1]);
    writeln(DB.numCookies);
    writeln(DB.numDimensions);

    return 0;
}

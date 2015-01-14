# HOWTO

## Using TrailDB To Analyze The Enron Emails

TrailDB was designed to be used with AdRoll cookies as keys,
but you can easily use any other 16-byte keys you like.

We'll demonstrate how to do this using the Enron Email Dataset,
by deriving keys from email addresses.

The point of this document is to teach you how to use TrailDB,
we make no claims about the validity of results regarding the Enron scandal.

### Before You Begin

Make sure you have TrailDB installed, we'll be using the Python bindings for it.
For more information about installing TrailDB,
see the README at <http://github.com/SemanticSugar/TrailDB>.

Next, download the Enron emails from <https://www.cs.cmu.edu/~./enron/>.

### TrailDB Primer

A TrailDB maps 16-byte keys to lists of events, sorted by time.
It was created by AdRoll to store user activity, grouped by cookie.
TrailDB compresses the hell out of event lists,
which makes it ideal for working with huge datasets
(i.e. billions of cookies and hundreds of billions of events per db).
But it can also be a convenient format for working with smaller sets of data,
like the one used by this tutorial.

### Storing Events In TrailDB

We're going to store trails by keys derived from email addresses.
Since our keys in TrailDB need to be 16-bytes,
we'll use a digest of each address, instead of the string itself.
Doing this also preserves the privacy of email addresses.
If we wanted to, we could also store an inverse mapping,
so that we could perform the reverse lookup of key to email address.

We're going to use email headers to generate simple timelines.
Every time we see an address in an email header, we'll call that an event.
Each event will have a type, an owner, a folder, a message-id, and a subject.
Remember, the point of this tutorial is to learn to use TrailDB,
so don't worry too much about what this actually means.

First, let's setup our code to parse the emails:

```python
import os
import sys
import email.parser
import email.utils
import hashlib
import traildb

def idir(path):
    for name in os.listdir(path):
        yield name, os.path.join(path, name)

def rdir(path):
    for name, path in idir(path):
        if os.path.isdir(path):
            for names, opath in rdir(path):
                yield (name,) + names, opath
        else:
            yield (), path

def messages(maildir):
    parser = email.parser.Parser()
    for user, userdir in idir(maildir):
        for names, path in rdir(userdir):
            folder = os.path.join(*names) if names else ''
            yield user, folder, parser.parse(open(path), True)
```

So if we call our `messages` function with the path of the `maildir`,
we'll get back an iterator with the user, folder and parsed email headers.
Perfect.

Now let's define our schema for events:

```python
def events(messages):
    for user, folder, message in messages:
        id = message['message-id']
        date = email.utils.parsedate_tz(message['date'])
        time = email.utils.mktime_tz(date)
        subject = message['subject']
        for type in ('to', 'from', 'cc', 'bcc'):
            for _, addr in email.utils.getaddresses(message.get_all(type, [])):
                yield addr, time, (type, user, folder, id, subject)
```

Now, if we pass the iterator returned by `messages` to `events`,
we'll get back an iterator of exactly the data we want to store in our TrailDB.

Next, let's define a function to create the database, given the events:

```python
def makedb(path, events):
    fields = ('type', 'owner', 'folder', 'message_id', 'subject')
    cons = traildb.TrailDBConstructor(path, fields)
    for addr, time, values in events:
        if time > 915091200: # dates before 1999 are too sparse / bogus
            cons.add(hashlib.md5(addr).hexdigest(), time, values)
    return cons.finalize()
```

Our `TrailDBConstructor` takes the desired output path,
and the list of fields we want to include in our database.
We can use the constructor it returns to add all of our events,
and when we are done we simply call `finalize` on the constructor.

You also probably noticed that we are removing dates before 1999.
The reason we must do this is that when TrailDB compresses the event lists,
it uses a 24-bit timedelta to shrink the entropy of the data.
With 24 bits, the largest timedelta we can express in seconds is ~194 days.
Since there are huge gaps in the data before 1999, we need to either filter,
or use a lower resolution timestamp (such as minutes or days).
It's easier to work with seconds everywhere,
and there's not much in the data before 1999, so here we simply filter.

Finally, let's turn this code into a command-line script:

```python
def main(path, maildir='maildir'):
    return makedb(path, events(messages(maildir)))

if __name__ == '__main__':
    main(*sys.argv[1:])
```

Save it in a file called `makedb.py` inside the Enron data directory.
From inside the Enron directory, let's call our script from the command line:

     python makedb.py enron.tdb

This should take a few minutes to run.
When it's done you should end up with a directory called `enron.tdb`,
and the following files inside it:

```
cookies
fields
info
lexicon.folder
lexicon.message_id
lexicon.owner
lexicon.subject
lexicon.type
trails.codebook
trails.data
trails.toc
```

This is what a typical traildb directory looks like.
The lexicon files are named after the fields you gave the constructor.

Let's quickly verify that our db works using the command line.
You can get some stats about the traildb using `tdb info`.
If you type:

     tdb info enron.tdb

You should see the following output:

```
enron.tdb/
 # cookies:                          87387
 # events:                         4769409
 # fields:                               6
 # -type:                                5
 # -owner:                             151
 # -folder:                           1938
 # -message_id:                     516289
 # -subject:                        158989
 > time:               1998-12-31 09:39:00
 < time:               2002-06-25 17:51:59
```

This tells us the total number of cookies (keys), the total number of events,
the number of fields and the number of values within each field (cardinality),
and finally the minimum and maximum timestamp of our events.

We can also verify that the lexicon, e.g. for `type`, looks correct:

    tdb lex -f type enron.tdb

Great!
Now let's take a look at what's really inside our database.

### Examining Trails

We can use the command line to dump the whole database using `tdb cat`,
or we can fetch individual rows like this:

    tdb get enron.tdb -i 56076 -f time -f type -f owner -f subject

Which selects id `56076`, and a subset of the fields.
That row happens to belong to Steven Kean, Enron Chief of Staff.

Let's write some code to compute who was the most active in a given time period:

```python
import datetime
import time
import traildb

def t(year, month, day):
    return time.mktime(datetime.datetime(year, month, day).timetuple())

def top(path, start=t(2001, 8, 7), end=t(2001, 8, 14)):
    return sorted((sum(1 for e in trail if start <= e.time < end), i)
                   for i, trail in enumerate(traildb.TrailDB(path)))

```

If we call this function on our database,
we can see Steven Kean appears to have been very active on email.

Let's do one more thing using the Python API.
We'll plot some of the information in the trails using [matplotlib](http://matplotlib.org/).

```python
from pylab import *
import traildb

tdb = traildb.TrailDB('enron.tdb')
hide = plot([e.time for e in tdb.trail(56076, ptime=True)],
            [e.type for e in tdb.trail(56076, expand=False)], 'o', alpha=.1)
```

You'll notice that we use two of the possible options for expanding trails in Python.
First we use the `ptime=True` option so that `e.time` is a `datetime` object and not an `int`.
Second we use the `expand=False` option so that `e.type` is an `int` and not a `str`.
This is a pretty silly example, except for the conspicuous gap in sent emails from April-July 2000.
How do you know `type=2` corresponds to sent emails? Check the lexicon:

     tdb.lexicon('type')

Since we noticed that gap, let's quickly check if anyone else exhibits suspicious behavior.
You can try the above with another user: `79772`.
Sure enough there's a huge gap in emails from June-October 2001,
right around the time when [shit hit the fan](http://www.nytimes.com/2006/01/18/business/worldbusiness/18iht-web.0117enron.time.html).
It's a good exercise to try writing a function that detects anomalies like these.

### Scaling Up

Although TrailDB can scale way beyond the size presented here,
there are still limitations on the size of a single TrailDB.
The number of keys, events and values in each lexicon all have limitations,
defined by `traildb.h`.
If you are worried about hitting these limits,
you should probably partition your data.
Before you hit any of these actual limits though,
you'll probably run out of memory.
In this case too, you should probably partition your data.

It's up to you how to partition your data,
but partitioning in the cookie (key) space is usually a good idea,
as it allows you to preserve entire trails for each cookie, within a single db.
TrailDB includes a generic hash function (`djb2`)
that works extremely well in practice for partitioning data by cookie.
If you use [crumby](https://github.com/SemanticSugar/crumby),
it can partition data automatically for you, using this hash function.

Finally, the Python API is one of the slowest ways imaginable to access TrailDB.
We used it here for demonstration purposes, but for heavy workloads,
you'll probably want to use either the `C`, `D`, or `Lua` bindings.
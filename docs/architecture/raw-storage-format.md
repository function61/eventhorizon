Raw storage format
==================

Pyramid stores all its log-formatted data in append-only text files. The good thing
about this design is that "resultset cursor" is just a byte offset into the chunk,
and all reads are just one `fseek()` away.


Lowest level format
-------------------

In lowest level, all lines look like this:

```
<line type> <line payload> <newline>
```

Line types (denoted by one byte, currently common chars in ASCII range) are used
to implement higher level datatypes. These are currently:

1. Meta event line (type "/")
2. Regular text line (type " ")

These low-level details will not leak to consumers, as the reader component parses
these implementation details into a higher-level representation.


1. Meta event line
------------------

Meta event lines look like this:

```
"/" <meta event type> <meta event payload>
```

Concrete example (\n omitted):

```
/Created {"subscription_ids": "/_sub/89a3c083", "ts":"2017-02-27T17:12:31.446Z"}
```


2. Regular text line
--------------------

These look like this:

```
" " <line content>
```

Concrete example:

```
 Lorem ipsum dolor sit amet, consectetur adipisicing elit.
```

Notice the leading space. The space was chosen to keep the 99 % case looking
as normal/noise-less as possible.


Encountering any other line type
--------------------------------

Encountering any other types should result in an error, because you cannot know
how to parse it. They are used for future extensibility.


Future extensibility
--------------------

We support extensibility by implementing new line types in the future. For example
we could support binary-safe content by allowing \n in the content:

- though that would mean more complex parsing than just splitting by \n
- for simplicity we would probably implement escape sequences for
  newline => "\n" and "\" => "\\"

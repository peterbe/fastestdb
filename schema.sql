CREATE TABLE talks (
id SERIAL,
topic varchar(200),
"when" timestamp,
tags text[],
duration real
);

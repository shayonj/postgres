# Regular-table analogue of detach-partition-concurrently behavior for FK drop.
#
# This spec exercises reduced relation-level locking (ShareRowExclusive) for
# FK/trigger removal:
# 1) SELECTs on the referenced table (pk) proceed while DROP CONSTRAINT runs.
# 2) Writers (UPDATE) on the referencing (fk) table block the FK drop.
# 3) Writers (UPDATE) on the referenced (pk) table block the FK drop.
# 4) Prepared SELECTs (plan + relcache) continue to work across FK drop.
# 5) DROP TABLE fktable doesn't block SELECTs on the referenced table.
# 6) Self-referential FK drops don't block SELECTs on the table.
# 7) ALTER COLUMN TYPE with FK rebuild doesn't block SELECTs on referenced table.

setup {
    drop table if exists pk, fk, fkself, fk_alttype;
    create table pk(id int primary key);
    create table fk(id int primary key, ref int references pk(id));
    create table fkself(id int primary key, parent int references fkself(id));
    create table fk_alttype(id int primary key, ref int references pk(id));
    insert into pk values (1);
    insert into fk values (1, 1);
    insert into fkself values (1, NULL);
    insert into fk_alttype values (1, 1);
}

teardown {
    drop table if exists fk, fkself, fk_alttype, pk;
}

session s1
step s1b            { begin; }
step s1s            { select * from pk; }
step s1declare_fk   { declare cfk cursor for select * from fk; }
step s1fetch_fk     { fetch 1 from cfk; }
step s1s_self       { select * from fkself; }
step s1wfk          { update fk set ref = ref where id = 1; }
step s1wpk          { update pk set id = id where id = 1; }
step s1w_self       { update fkself set id = id where id = 1; }
step s1c            { commit; }
step s1prep         { prepare q as select count(*) from pk; }
step s1exec         { execute q; }

session s2
step s2drop         { alter table fk drop constraint fk_ref_fkey; }
step s2drop_table   { drop table fk; }
step s2drop_self    { alter table fkself drop constraint fkself_parent_fkey; }
step s2alter_type   { alter table fk_alttype alter column ref type bigint; }

# (1) Readers do not block FK drop
permutation s1b s1s s2drop s1c

# (1a) SELECT on referencing table blocks FK drop (ALTER TABLE waits)
permutation s1b s1declare_fk s1fetch_fk s2drop s1c

# (2) Writer on referencing table blocks FK drop
permutation s1b s1wfk s2drop s1c

# (3) Writer on referenced table blocks FK drop
permutation s1b s1wpk s2drop s1c

# (4) Prepared plan remains usable across FK drop; relcache invalidation ensures correctness
permutation s1prep s1b s1exec s2drop s1exec s1c

# (5) DROP TABLE fktable does not block SELECTs on referenced table
permutation s1b s1s s2drop_table s1c

# (6) Self-referential FK drop: ALTER TABLE takes AccessExclusive on the table
# being altered, so even SELECTs block (different from non-self-ref case)
permutation s1b s1s_self s2drop_self s1c

# (7) Writer on self-referential FK table blocks drop of self-referential FK
permutation s1b s1w_self s2drop_self s1c

# (8) ALTER COLUMN TYPE rebuilding FK: SELECTs on referenced table proceed
permutation s1b s1s s2alter_type s1c

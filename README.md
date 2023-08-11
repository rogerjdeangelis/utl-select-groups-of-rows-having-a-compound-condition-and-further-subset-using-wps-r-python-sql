# utl-select-groups-of-rows-having-a-compound-condition-and-further-subset-using-wps-r-python-sql
Select groups of rows having a compound condition and further subset using wps r python sql
    %let pgm =utl-select-groups-of-rows-having-a-compound-condition-and-further-subset-using-wps-r-python-sql;

    Select groups of rows having a compound condition and further subset using wps r python sql

    github
    https://tinyurl.com/284xs44y
    https://github.com/rogerjdeangelis/utl-select-groups-of-rows-having-a-compound-condition-and-further-subset-using-wps-r-python-sql

      Solutions

         1 wps sql
         2 wps r sql
         3 wps python sql

    I could not get any of the posted R solutions to work.

    https://stackoverflow.com/questions/76876788/remove-rows-if-a-certain-condition-occur

    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */

    libname sd1 "d:/sd1";

    data sd1.have;informat
    COUNTRY $6.
    YEAR 8.
    X 8.
    ;input
    COUNTRY YEAR X;
    cards4;
    France 2000 1
    France 2001 2
    France 2002 3
    France 2003 4
    Italy 2000 5
    Italy 2001 6
    Italy 2002 7
    Italy 2003 8
    Spain 2000 9
    Spain 2001 10
    Spain 2002 11
    Spain 2003 12
    ;;;;
    run;quit;

    /**************************************************************************************************************************/
    /*                               |                                        |                                               */
    /*                               |                                        |                                               */
    /* SD1.HAVE total obs=12         |  PROCESS                               | OUTPUT                                        */
    /*                               |                                        |                                               */
    /* bs    COUNTRY    YEAR     X   |                                        |   COUNTRY      YEAR         X                 */
    /*                               |                                        |   ---------------------------                 */
    /*  1    France     2000     1   |  After removing Spain                  |   France       2000         1                 */
    /*  2    France     2001     2   |                                        |   France       2001         2                 */
    /*  3    France     2002     3   |  select rows that are not              |   France       2002         3 Has 2002 x<=7   */
    /*  4    France     2003     4   |                                        |   France       2003         4                 */
    /*                               |  not (X > 7 and  year = 2002)          |                                               */
    /*  5    Italy      2000     5   |                                        |   Italy        2000         5                 */
    /*  6    Italy      2001     6   |                                        |   Italy        2001         6                 */
    /*  7    Italy      2002     7   |                                        |   Italy        2002         7  Has 2002 x<=7  */
    /*  8    Italy      2003     8   |                                        |   Italy        2003         8                 */
    /*                               |                                        |                                               */
    /*  9    Spain      2000     9   |  Remove SPAIN because it does not have |   Keep X=8 because NOT 2002 and x > 7.        */
    /* 10    Spain      2001    10   |                                        |   2003 is enough to decide                    */
    /* 11    Spain      2002    11   |  zt lease one ( year = 2002 and x<=7)  |                                               */
    /* 12    Spain      2003    12   |                                        |                                               */
    /*                               |                                        |                                               */
    /**************************************************************************************************************************/

    /*                                  _
    / | __      ___ __  ___   ___  __ _| |
    | | \ \ /\ / / `_ \/ __| / __|/ _` | |
    | |  \ V  V /| |_) \__ \ \__ \ (_| | |
    |_|   \_/\_/ | .__/|___/ |___/\__, |_|
                 |_|                 |_|
    */
    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('

    libname sd1 "d:/sd1";

    options validvarname=any;

    proc sql;
      create
         table sd1.want as
      select
         l.country
        ,l.year
        ,l.x
      from
        sd1.have as l, (
          select
             country
          from
             sd1.have
          having
            ( year = 2002 and x<=7)
          ) as r
      where
              l.country = r.country
          and not ( l.year = 2002 and l.x > 7)
    ;quit;
    proc print data=sd1.want;
    run;quit;

    ');

    /*           _               _
      ___  _   _| |_ _ __  _   _| |_
     / _ \| | | | __| `_ \| | | | __|
    | (_) | |_| | |_| |_) | |_| | |_
     \___/ \__,_|\__| .__/ \__,_|\__|
                    |_|
    */

    /**************************************************************************************************************************/
    /*                               |                                                                                        */
    /* The WPS System                |  The inn select results in                                                             */
    /*                               |                                                                                        */
    /* Obs    COUNTRY    YEAR    X   |   COUNTRY                                                                              */
    /*                               |                                                                                        */
    /*  1     France     2003    4   |   France                                                                               */
    /*  2     France     2002    3   |   Italy                                                                                */
    /*  3     France     2001    2   |                                                                                        */
    /*  4     France     2000    1   |   Spain is dropped  because it does not have at least one                              */
    /*                               |                                                                                        */
    /*  5     Italy      2003    8   |   year = 2002 and x<=7                                                                 */
    /*  6     Italy      2002    7   |                                                                                        */
    /*  7     Italy      2001    6   |   The outer select just does the final filtering                                       */
    /*  8     Italy      2000    5   |                                                                                        */
    /*                               |                                                                                        */
    /**************************************************************************************************************************/

    /*___                                          _
    |___ \  __      ___ __  ___   _ __   ___  __ _| |
      __) | \ \ /\ / / `_ \/ __| | `__| / __|/ _` | |
     / __/   \ V  V /| |_) \__ \ | |    \__ \ (_| | |
    |_____|   \_/\_/ | .__/|___/ |_|    |___/\__, |_|
                     |_|                        |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('

    libname sd1 "d:/sd1";

    proc r;
    export data=sd1.have r=have;
    submit;
    library(sqldf);
    want <- sqldf("
      select
         l.country
        ,l.year
        ,l.x
      from
        have as l, (
          select
             max(country) as country
          from
             have
          group
             by country
          having
            max( year = 2002 and x<=7)
          ) as r
      where
              l.country = r.country
          and  ( l.year = 2002 and l.x > 7) = 0
      ");
    want;
    endsubmit;
    run;quit;
    ');


    /*____                                    _   _                             _
    |___ /  __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
      |_ \  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
     ___) |  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
    |____/    \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
                     |_|         |_|    |___/                                |_|
    */

    %utl_submit_wps64x('

    libname sd1 "d:/sd1";
    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    proc python;
    export data=sd1.have python=have;
    submit;
     from os import path;
     import pandas as pd;
     import numpy as np;
     import pandas as pd;
     from pandasql import sqldf;
     mysql = lambda q: sqldf(q, globals());
     from pandasql import PandaSQL;
     pdsql = PandaSQL(persist=True);
     sqlite3conn = next(pdsql.conn.gen).connection.connection;
     sqlite3conn.enable_load_extension(True);
     sqlite3conn.load_extension("c:/temp/libsqlitefunctions.dll");
     mysql = lambda q: sqldf(q, globals());
     want=pdsql("""
      select
         l.country
        ,l.year
        ,l.x
      from
        have as l, (
          select
             max(country) as country
          from
             have
          group
             by country
          having
            max( year = 2002 and x<=7) = 1
          ) as r
      where
              l.country = r.country
          and  ( l.year = 2002 and l.x > 7) = 0
     """);
    print(want);
    endsubmit;
    import data=sd1.want python=want;
    run;quit;
    proc print data=sd1.want;
    run;quit;
    ');

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */

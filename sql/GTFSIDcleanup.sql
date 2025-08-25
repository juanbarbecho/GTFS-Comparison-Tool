WITH precleaned AS ( -- Before getting into name fixes, fix edge cases that involve trains running on unusual lines
    SELECT
        CASE
	        -- Cases when the Q runs on the N line in brooklyn, and stop names are common
	        WHEN route_name = 'Q_0_N..N63R_3999' AND stop_name = '36 St' THEN 'R36'  || DIR
	        WHEN route_name = 'Q_0_N..N63R_3999' AND stop_name = '59 St' THEN 'R41' || DIR
	        WHEN route_name = 'Q_0_N..N63R_3999' AND stop_name = '8 Av' THEN 'N02' || DIR
	        WHEN route_name = 'Q_0_N..N63R_3999' AND stop_name = 'Fort Hamilton Pkwy' THEN 'N03' || DIR
	        WHEN route_name = 'Q_0_N..N63R_3999' AND stop_name = 'New Utrecht Av' THEN 'N04' || DIR
	        WHEN route_name = 'Q_0_N..N63R_3999' AND stop_name = '18 Av' THEN 'N05' || DIR
	        WHEN route_name = 'Q_0_N..N63R_3999' AND stop_name = '20 Av' THEN 'N06' || DIR
	        WHEN route_name = 'Q_0_N..N63R_3999' AND stop_name = 'Bay Pkwy' THEN 'N07' || DIR
            WHEN route_name = 'Q_0_N..N63R_3999' AND stop_name = 'Kings Hwy' THEN 'N08' || DIR
            WHEN route_name = 'Q_0_N..N63R_3999' AND stop_name = 'Avenue U' THEN 'N09' || DIR
            WHEN route_name = 'Q_0_N..N63R_3999' AND stop_name = '86 St' AND milepost = 1.0822099447250366 THEN 'N10' || DIR
            WHEN route_name = 'Q_0_N..N63R_3999' AND stop_name = '86 St' AND milepost = 17.645967483520508 THEN 'Q04' || DIR
            -- Cases when the name mismatches due to the addition of the '(2 Av)', manual fix
            WHEN route = 'Q' AND stop_name = '86 St (2 Av)' THEN 'Q04' || DIR
            
            -- Cases when the M runs on the Q line in the upper east side
            WHEN route_name in ('M..N96St','M..S96St') AND stop_name = '96 St' THEN 'Q05' || DIR
            WHEN route_name in ('M..N96St','M..S96St') AND stop_name = '86 St' THEN 'Q04' || DIR
            WHEN route_name in ('M..N96St','M..S96St') AND stop_name = '72 St' THEN 'Q03' || DIR
            WHEN route_name in ('M..N96St','M..S96St') AND stop_name = 'Lexington Av/63 St' THEN 'B08' || DIR
            WHEN route_name in ('M..N96St','M..S96St') AND stop_name = '57 St/7 Av' THEN 'B10' || DIR
            -- Cases when the M terminates at 57 St
            WHEN route_name IN ('M_0__377430','M_1__377451') AND stop_name = '57 St' THEN 'B10' || DIR
            
            -- Cases when the F runs on the E line in Long Island City and 53rd St
			WHEN route IN ('F','FX') AND stop_name = 'Queens Plaza' THEN 'G21' || DIR
			WHEN route IN ('F','FX') AND stop_name = 'Court Sq-23 St' THEN 'F09' || DIR
			WHEN route IN ('F','FX') AND stop_name = 'Lexington Av/53 St' THEN 'F11' || DIR
			WHEN route IN ('F','FX') AND stop_name = '5 Av/53 St' THEN 'F12' || DIR
			
			-- Cases when the name mismatches due to the addition of the '(Brighton)',manual fix
			WHEN route = 'B' AND stop_name = '7 Av (Brighton)' THEN 'D25' || DIR
			
			-- Cases when the name mismatches due to the addition of the '(2 Av)', manual fix
            WHEN route in ('R','N') AND stop_name = '86 St (2 Av)' THEN 'Q04' || DIR
            
            -- Cases when the name mismatches due to the addition of the '(4 Av)', manual fix
            WHEN route = 'R' AND stop_name = '36 St (4 Av)' THEN 'R36' || DIR
            
            -- Cases when the name mismatches due to the addition of the '(4 Av)', manual fix
            WHEN route = 'R' AND stop_name = '36 St (Queens Blvd)' THEN 'G20' || DIR
            
            -- Cases when the name mismatches due to the addition of the '(White Plains Rd)', manual fix 
            WHEN route IN ('2','5') AND stop_name = 'Gun Hill Rd (White Plains Rd)' THEN '208' || DIR
            WHEN route IN ('2','5') AND stop_name = 'Pelham Pkwy (White Plains Rd)' THEN '211' || DIR
            
            -- Cases when the name mismatches due to the addition of the '(Dyre Av)', manual fix
            WHEN route IN ('2','5') AND stop_name = 'Gun Hill Rd (Dyre Av)' THEN '503' || DIR
            WHEN route IN ('2','5') AND stop_name = 'Pelham Pkwy (Dyre Av)' THEN '504' || DIR
            
            ELSE mhr.gtfsid
        END AS GTFSID,  -- overwrite GTFSID
        mhr.RIDERSHIP_RUN_ID,
        mhr.RUN_YEAR,
        mhr.RUN_MONTH,
        mhr.WEEKEND,
        mhr.HOUR, 
        mhr.ROUTE_NAME,
        mhr.ROUTE,
        mhr.DIR,
        mhr.STOP_NAME,
        mhr.LOOKUP,
        mhr.MILEPOST,
        mhr.ONS,
        mhr.offs,
        mhr.volume,
        mhr.MD_TRIPS,
        mhr.CAPACITY,
        mhr.VC
    FROM RAIL."MD_24H-RIDERSHIP" mhr WHERE gtfsid != '0' -- disregard those that are bad runs
) 
, good_id AS ( -- extract good ids from october 2024, with edge cases manually filled in
    SELECT GTFSID, ROUTE, DIR, STOP_NAME
    FROM precleaned
    WHERE RUN_YEAR = '2024'
      AND RUN_MONTH = '10'
      AND GTFSID IS NOT NULL
)
, missing AS ( -- missing gtfsids
    SELECT DISTINCT STOP_NAME, ROUTE, DIR
    FROM precleaned
    WHERE GTFSID IS NULL
)
, match_attempt AS ( -- first match attempt that tries to just match on stop name, route, and direction
    SELECT m.*, g.GTFSID AS matched_gtfsid
    FROM missing m
    LEFT JOIN good_id g
      ON m.STOP_NAME = g.STOP_NAME
     AND m.ROUTE = g.ROUTE
     AND m.DIR = g.DIR
)
, mismatches AS ( -- failed match attempts, all due to naming differences
    SELECT *
    FROM match_attempt
    WHERE matched_gtfsid IS NULL
)
, renamed AS ( -- fixing naming differences
    SELECT ROUTE, STOP_NAME, DIR,
        CASE
            WHEN STOP_NAME = 'Broadway Jct' THEN 'Broadway Junction'
            WHEN STOP_NAME = 'Aqueduct-North Conduit Av' THEN 'Aqueduct-N Conduit Av'
            WHEN STOP_NAME = 'Briarwood-Van Wyck Blvd' THEN 'Briarwood'
            WHEN STOP_NAME = 'Christopher St-Sheridan Sq' THEN 'Christopher St-Stonewall'
            WHEN STOP_NAME = 'Franklin Av' AND route IN ('2','3','4','5') THEN 'Franklin Av-Medgar Evers College'
            WHEN STOP_NAME = 'Hoyt-Schermerhorn' THEN 'Hoyt-Schermerhorn Sts'
            WHEN STOP_NAME = 'Whitehall St' THEN 'Whitehall St-South Ferry'
            WHEN STOP_NAME = 'Kosciusko St' THEN 'Kosciuszko St'
            WHEN STOP_NAME = 'World Trade Center' AND ROUTE = '1' THEN 'WTC Cortlandt'
            WHEN STOP_NAME = 'Park Pl' AND route IN ('2','3') THEN 'Park Place'
            WHEN STOP_NAME = 'President St' THEN 'President St-Medgar Evers College'
            WHEN STOP_NAME = 'Newkirk Av' AND route IN ('2','5') THEN 'Newkirk Av-Little Haiti'
            WHEN STOP_NAME = 'Newkirk Av' AND route IN ('B','Q') THEN 'Newkirk Plaza'
            WHEN STOP_NAME = 'Ocean Pky' THEN 'Ocean Pkwy'
            WHEN STOP_NAME = 'South Ferry Terminal' THEN 'South Ferry'
            WHEN STOP_NAME = '39 Av' THEN '39 Av-Dutch Kills'
            WHEN STOP_NAME IN ('Union Sq-14 St', '14 St - Union Sq') THEN '14 St-Union Sq'
            WHEN STOP_NAME = 'Times Sq - 42 St' THEN 'Times Sq-42 St'
            WHEN STOP_NAME = 'Beverly Rd' AND route = 'Q' THEN 'Beverley Rd'
            WHEN STOP_NAME = '9 St' OR STOP_NAME = '4 Av' THEN '4 Av-9 St'
            WHEN STOP_NAME = '8 St - NYU' THEN '8 St-NYU'
            WHEN STOP_NAME = '168 St' AND route = '1' THEN '168 St-Washington Hts'
            WHEN STOP_NAME = '34 St' AND route = '7' THEN '34 St-Hudson Yards'
            WHEN STOP_NAME = 'E 105 St' THEN 'East 105 St'
            WHEN STOP_NAME = 'Atlantic Av' AND route = 'Q' THEN 'Atlantic Av-Barclays Ctr'
            WHEN STOP_NAME = '75 St' THEN '75 St-Elderts Ln'
            WHEN STOP_NAME = '34 St - Herald Sq' THEN '34 St-Herald Sq'
            WHEN STOP_NAME IN ('Essex St','Delancey St') THEN 'Delancey St-Essex St'
            WHEN STOP_NAME = '36 St (Queens Blvd)' AND route != 'R' THEN '36 St'
            WHEN STOP_NAME = '36 St (4 Av)' AND route != 'R' THEN '36 St'
            WHEN STOP_NAME IN ('57 St - 7 Av', '57 St/7 Av') AND route != 'M' THEN '57 St-7 Av'
            WHEN STOP_NAME IN ('7 AV(53 St)' , '7 Av (53 St)') THEN '7 Av'
            WHEN STOP_NAME IN ('Queensboro Plaza (Express)','Queensboro Plaza (Local)') AND route = '7' THEN 'Queensboro Plaza'
            WHEN STOP_NAME IN ('61 St-Woodside (Express)','61 St-Woodside (Local)') AND route = '7' THEN '61 St-Woodside'
            ELSE STOP_NAME
        END AS fixed_stop_name
    FROM mismatches
)
, renamedmatches AS ( --match again with fixed names
    SELECT DISTINCT r.*, g.GTFSID
    FROM renamed r
    LEFT JOIN good_id g
      ON r.route = g.route
     AND r.fixed_stop_name = g.STOP_NAME
     AND r.DIR = g.DIR
)
, all_filled AS ( --fill in gtfsids from match attempts
    SELECT
        p.*,
        COALESCE(p.gtfsid, ma.matched_gtfsid, rm.GTFSID) AS final_gtfsid
    FROM precleaned p
    LEFT JOIN match_attempt ma
      ON p.stop_name = ma.stop_name
     AND p.route = ma.route
     AND p.dir = ma.dir
    LEFT JOIN renamedmatches rm
      ON p.stop_name = rm.stop_name
     AND p.route = rm.route
     AND p.dir = rm.dir
)
SELECT * 
FROM all_filled
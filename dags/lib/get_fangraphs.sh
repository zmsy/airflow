# get batters projections html
curl 'https://www.fangraphs.com/projections.aspx?pos=all&stats=bat&type=fangraphsdc' \
    -H 'Accept-Encoding: gzip, deflate, br' \
    -H 'Accept-Language: en-US,en;q=0.9' \
    -H 'Upgrade-Insecure-Requests: 1' \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8' \
    -d @$AIRFLOW_HOME/dags/lib/batters_form_data.txt --compressed \
    > $AIRFLOW_HOME/output/batters_projections_depth_charts.html

# get batters projections html - rest of season
curl 'https://www.fangraphs.com/projections.aspx?pos=all&stats=bat&type=rfangraphsdc' \
    -H 'Accept-Encoding: gzip, deflate, br' \
    -H 'Accept-Language: en-US,en;q=0.9' \
    -H 'Upgrade-Insecure-Requests: 1' \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8' \
    -d @$AIRFLOW_HOME/dags/lib/batters_form_data.txt --compressed \
    > $AIRFLOW_HOME/output/batters_projections_depth_charts_ros.html

# get pitching projections
curl 'https://www.fangraphs.com/projections.aspx?pos=all&stats=pit&type=fangraphsdc' \
    -H 'Accept-Encoding: gzip, deflate, br' \
    -H 'Accept-Language: en-US,en;q=0.9' \
    -H 'Upgrade-Insecure-Requests: 1' \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8' \
    -d @$AIRFLOW_HOME/dags/lib/pitchers_form_data.txt --compressed \
    > $AIRFLOW_HOME/output/pitchers_projections_depth_charts.html

# get pitching projections - rest of season
curl 'https://www.fangraphs.com/projections.aspx?pos=all&stats=pit&type=rfangraphsdc' \
    -H 'Accept-Encoding: gzip, deflate, br' \
    -H 'Accept-Language: en-US,en;q=0.9' \
    -H 'Upgrade-Insecure-Requests: 1' \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8' \
    -d @$AIRFLOW_HOME/dags/lib/pitchers_form_data.txt --compressed \
    > $AIRFLOW_HOME/output/pitchers_projections_depth_charts_ros.html

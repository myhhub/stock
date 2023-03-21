

rm -f ./js/bokeh.min.js
rm -f ./js/bokeh-api.min.js
rm -f ./js/bokeh-gl.min.js
rm -f ./js/bokeh-tables.min.js
rm -f ./js/bokeh-widgets.min.js

cp /usr/local/lib/python3.7/site-packages/bokeh/server/static/js/bokeh.min.js ./js/
cp /usr/local/lib/python3.7/site-packages/bokeh/server/static/js/bokeh-api.min.js ./js/
cp /usr/local/lib/python3.7/site-packages/bokeh/server/static/js/bokeh-gl.min.js ./js/
cp /usr/local/lib/python3.7/site-packages/bokeh/server/static/js/bokeh-tables.min.js ./js/
cp /usr/local/lib/python3.7/site-packages/bokeh/server/static/js/bokeh-widgets.min.js ./js/
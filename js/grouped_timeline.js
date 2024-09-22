var options = {
  series: [],
  chart: {
    height: 500,
    type: "rangeBar",
  },
  plotOptions: {
    bar: {
      horizontal: true,
      barHeight: "50%",
      rangeBarGroupRows: true,
    },
  },
  colors: [],

  fill: {
    type: "solid",
  },
  xaxis: {
    type: "datetime",
  },
  legend: {
    position: "right",
  },
  noData: {
    text: "No Data....",
  },

  tooltip: {
    // NOTE: function is triggered when hover over object in chart
    custom: function ({ series, seriesIndex, dataPointIndex, w }) {
      // TODO: display begin tim, not end time. maybe display both actually in nice tooltip
      let d2 = new Date(series[seriesIndex][dataPointIndex]);
      let d = new Date(d2.getTime() + d2.getTimezoneOffset() * 60 * 1000);
      console.log(d);
      console.log(d2);
      // console.log(d.toISOString());
      // console.log(w.config.series);
      // console.log(w.config.series[seriesIndex].name);
      // console.log(series[seriesIndex][dataPointIndex]);
      // console.log(series, seriesIndex, dataPointIndex);
      return (
        '<div class="arrow_box">' +
        "<span>" +
        w.config.series[seriesIndex].name +
        ": " +
        `${d.getFullYear()}-${d.getMonth()}-${d.getDate()} ${d.getHours()}:${d.getMinutes()}:${d.getSeconds()}.${d.getMilliseconds()}` +
        // d +
        "</span>" +
        "</div>"
      );
    },
  },
};

// create empty chart
var chart = new ApexCharts(document.querySelector("#grp_timeline"), options);
chart.render();

// populate chart test
// var url = "http://my-json-server.typicode.com/apexcharts/apexcharts.js/yearly";
var url =
  "http://127.0.0.1:8000/timeline_viz?locid=1&date=2024-09-20&time=0900&addhrs=1";

axios({
  method: "GET",
  url: url,
}).then(function (response) {
  console.log(response);
  // chart.updateSeries(response.data);
  chart.updateOptions(response.data);
});

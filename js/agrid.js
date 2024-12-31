import { API_URL, APP_URL } from "./env.js";

const locationInput = document.getElementById("locations");
const locationDlist = document.getElementById("location-list");
const start_dtInput = document.getElementById("start_dt");
const end_dtInput = document.getElementById("end_dt");
const getdataBtn = document.getElementById("getdataBtn");
const noDataNotification = document.getElementById("noDataNotification");

/**=======================
 *      Util functions
 *
 *========================**/

// USED TO ADD ZERO TO DATE FUNCS LIKE getDate()
function addzero(int) {
  if (int < 10) {
    return "0" + int.toString();
  }
  return int.toString();
}

/**
 * Adds hours to datestring.
 * Returns Local ISO datetime to minute precision, typ used for date select input
 *
 * @param {string} datestring - format (2024-12-25T00:00)
 * @param {string} hr - number of hours to add
 * @returns {string} ISO datetime to minute precision
 */
function addHrs(datestring, hr) {
  const utc_date_obj = new Date(datestring);
  const tz_offset_ms = utc_date_obj.getTimezoneOffset() * 60 * 1000;
  const hr_ms = 1000 * 60 * 60 * hr;

  const local_epoch_ms = utc_date_obj.getTime() - tz_offset_ms + hr_ms;
  const new_date_obj = new Date(local_epoch_ms);
  return new_date_obj.toISOString().slice(0, 16);
}

/**======================
 *    Set Start & End Datetime Inputs
 * default times
 *========================**/
let local_date = new Date();
start_dtInput.value = `${local_date.getFullYear()}-${addzero(
  local_date.getMonth() + 1
)}-${addzero(local_date.getDate())}T00:00`;

end_dtInput.value = addHrs(start_dtInput.value, 1);

/**============================================
 * *     Fetch location dropdown info
 *=============================================**/
let locObject;
let locReverse;

async function fetchLocids() {
  try {
    let response = await fetch(
      `${API_URL}/form_locids`
      // NOTE: allow origins * cannot be used with credential include
      // , {credentials: "include", }
    );

    // TODO: Do not like roundabout way for getting locid/location info for location input, find better code
    const locInfo = await response.json();
    locObject = locInfo[0];
    locReverse = locInfo[1];

    // Add data to locations datalist
    for (const [k, v] of Object.entries(locObject)) {
      let newOption = document.createElement("option");
      newOption.value = k;
      locationDlist.appendChild(newOption);
    }
  } catch (error) {
    console.error(error.message);
    console.error(response.status);
  }
}

/**============================================
 *               Fetch grid data function
 *=============================================**/

async function fetch_griddata() {
  // default datetime has 'T' format; remove and split date/hour
  let sdt = start_dtInput.value.replace("T", " ");
  let edt = end_dtInput.value.replace("T", " ");

  getdataBtn.classList.add("is-loading");

  try {
    let locid = locObject[locationInput.value];
    let response = await fetch(
      `${API_URL}/hiresgrid?locid=${locid}&startdt=${sdt}&enddt=${edt}`
    );

    // list of dictionaries
    let gridDataObj = await response.json();

    // CHECK IF DATA WAS RETURNED; IF NOT SHOW NOTIFICATION BANNER
    if (Object.keys(gridDataObj).length === 0) {
      noDataNotification.querySelector("p").textContent =
        "No Data found for submitted location, date and time.";
      noDataNotification.classList.remove("is-hidden");
    } else {
      // Remove user notifications
      getdataBtn.classList.remove("is-loading");
      noDataNotification.classList.add("is-hidden");
    }

    // FILL AG GRID WITH DATA
    gridApi.setGridOption("rowData", gridDataObj);
  } catch (error) {
    // TODO: clear aggrid to make more apparent to user of issue with request
    noDataNotification.querySelector("p").textContent =
      "Error Processing Data!";
    noDataNotification.classList.remove("is-hidden");

    getdataBtn.classList.remove("is-loading");
    console.error(error.message);
  }
}

/**============================================
 * *              AG Grid Code
 *=============================================**/

// Event codes to color text red
let danger_arr = [
  "173-1", // unit flash events (173)
  "173-2",
  "173-3",
  "173-4",
  "173-5",
  "173-6",
  "173-7",
  "173-8",
  "200-15", //flash alarm
  "201-15",
  "180-0", // Stop time
  "180-1",
  "179-0", // Interval advance
  "179-1",
  "200-21", // SDLC Fault
  "201-21",
  "200-31", // Cabinet Flash
  "201-31",
  "200-20", // Controller Fault
  "201-20",
  "200-23", // TermFacility SDLC fault
  "201-23",
];

// Event codes to color text blue
let ops_arr = [
  "182-1", // power failure
  "184-1", // power restored
  "200-1", // power up alarm
  "201-1",
  "200-5",
  "201-5",
  "200-73",
  "201-73",
];

// Non Parameter dependant Event codes to color text blue
let ops_arr2 = [
  102, // preempt input on
  104,
  105, // preempt entry
  106, // preempt track clear
  107, // preempt dwell
  111, // preempt exit
];

let green_arr_start = [1, 61, 62];
let amber_arr_start = [8, 63];
let red_arr_start = [10, 64];

// Grid Options: Contains all of the Data Grid configurations
const gridOptions = {
  // Row Data: The data to be displayed. init w/no data
  rowData: [],

  // Column Definitions: Defines the columns to be displayed.
  columnDefs: [
    { field: "loc_id", headerName: "LocID" },
    { field: "dt", headerName: "Datetime" },
    { field: "event_code", headerName: "Event Code" },
    { field: "parameter", headerName: "Parameter" },
    {
      field: "event_descriptor",
      headerName: "Event Descriptor",
      cellClassRules: {
        "rag-red": (params) => red_arr_start.includes(params.data.event_code),
        "rag-amber": (params) =>
          amber_arr_start.includes(params.data.event_code),
        "rag-green": (params) =>
          green_arr_start.includes(params.data.event_code),
      },
    },
    { field: "phase_status", headerName: "Phase Status" },
    { field: "ovl_status", headerName: "Overlap Status" },
    { field: "ops_status", headerName: "Operational Status" },
    { field: "time_grp", headerName: "Time Group", hide: true },
    {
      field: "time_increment",
      filter: "agNumberColumnFilter",
      headerName: "Time Increment (sec)",
    },
  ],

  autoSizeStrategy: {
    type: "fitCellContents",
    // type: "fitGridWidth",
    // defaultMinWidth: 100,
  },

  rowClassRules: {
    "border-amber-start": (params) =>
      amber_arr_start.includes(params.data.event_code),

    "border-red-start": (params) =>
      red_arr_start.includes(params.data.event_code),

    "border-green-start": (params) =>
      green_arr_start.includes(params.data.event_code),

    "rag-danger": (params) =>
      danger_arr.includes(
        params.data.event_code.toString() +
          "-" +
          params.data.parameter.toString()
      ),

    "rag-ops": (params) =>
      ops_arr.includes(
        params.data.event_code.toString() +
          "-" +
          params.data.parameter.toString()
      ),
    "rag-ops2": (params) => ops_arr2.includes(params.data.event_code),

    "rag-time": (params) => params.data.time_grp == "x",
  },

  rowSelection: {
    mode: "multiRow",
    checkboxes: true,
    enableClickSelection: true,
    enableSelectionWithoutKeys: true,
  },

  defaultColDef: {
    filter: true,
  },
};

/**============================================
 **               Event Listners
 *=============================================**/
let gridApi;

document.addEventListener("DOMContentLoaded", function () {
  /**======================
   *    Create AG Grid
   *========================**/
  // NOTE: setup the grid after the page has finished loading
  // Need this for my async fetch calls to update grid
  var gridDiv = document.querySelector("#grid");
  gridApi = agGrid.createGrid(gridDiv, gridOptions);

  /**======================
   *    Notification Banner
   *========================**/
  // Click event to close notifications
  (document.querySelectorAll(".notification") || []).forEach(
    ($notification) => {
      $notification.addEventListener("click", () => {
        $notification.classList.add("is-hidden");
      });
    }
  );

  /**======================
   *    Get Data Button
   *========================**/
  getdataBtn.addEventListener("click", async function () {
    // Validate start and end datetimes
    if (start_dtInput.value > end_dtInput.value) {
      end_dtInput.classList.add("is-danger");
      return 0;
    }
    end_dtInput.classList.remove("is-danger");

    await fetch_griddata();

    // Set newURl in browser but do not reload page
    // Used in case user wants to copy and send as link
    let locid = locObject[locationInput.value];
    let newURL = `${APP_URL}/?startdt=${start_dtInput.value}&enddt=${end_dtInput.value}&locid=${locid}`;
    window.history.pushState({}, "", newURL);
  });

  /**======================
   *    Populate end date
   * double click, add 1 hr to start datetime
   *========================**/
  end_dtInput.addEventListener("dblclick", function () {
    end_dtInput.value = addHrs(start_dtInput.value, 1);
  });

  /**======================
   *    Populate start date
   * double click, substract 1 hr to end datetime
   *========================**/
  start_dtInput.addEventListener("dblclick", function () {
    start_dtInput.value = addHrs(end_dtInput.value, -1);
  });

  /**======================
   *    Validate Location Input
   * Turn input red if selection not in
   * location object
   *========================**/
  locationInput.addEventListener("change", (event) => {
    // console.log("Input value:", event.target.value);
    locationInput.classList.remove("is-danger");
    if (!locObject.hasOwnProperty(event.target.value)) {
      // if (!arr.includes(event.target.value)) {
      locationInput.classList.add("is-danger");
    }
  });
});

// AFTER ALL SCRIPT AND WINDOWS LOADED;
window.onload = async function () {
  // **onload WAITs TO LOAD LOCATIONS IN Locations SELECT ELEMENT
  await fetchLocids();

  // IF QUERY PARAMETERS PASSED, SET location and datetimes
  // USED for automated links that may be triggered by other apps
  let searchParams = new URLSearchParams(window.location.search);

  if (searchParams.size > 0) {
    //  Set values with .get() method
    locationInput.value = locReverse[searchParams.get("locid")];
    start_dtInput.value = searchParams.get("startdt");
    end_dtInput.value = searchParams.get("enddt");

    fetch_griddata();
  }
};

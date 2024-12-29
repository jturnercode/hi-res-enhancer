import { API_URL, APP_URL } from "./env.js";

const locationSel = document.getElementById("locations");
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
async function fetchLocids() {
  try {
    let response = await fetch(
      `${API_URL}/form_locids`
      // NOTE: allow origins * cannot be used with credential include
      // , {credentials: "include", }
    );
    if (!response.ok) {
      throw new Error(`Response status: ${response.status}`);
    }

    let locObject = await response.json();

    // add data to locations dropdown
    for (const [k, v] of Object.entries(locObject)) {
      let newOption = new Option(v.name, v.atms_id);
      locationSel.add(newOption);
    }
  } catch (error) {
    console.error(error.message);
  }
}

/**============================================
 *               Fetch grid data function
 *=============================================**/

async function fetch_griddata() {
  // TODO: ***add exception catch, return for ag grid
  console.log("getting data------->");

  // default datetime has 'T' format; remove and split date/hour
  let sdt = start_dtInput.value.replace("T", " ");
  let edt = end_dtInput.value.replace("T", " ");

  getdataBtn.classList.add("is-loading");

  let response = await fetch(
    `${API_URL}/hiresgrid?locid=${locationSel.value}&startdt=${sdt}&enddt=${edt}`
  );

  // list of dictionaries
  let gridDataObj = await response.json();

  getdataBtn.classList.remove("is-loading");

  // CHECK IF DATA WAS RETURNED; IF NOT SHOW NOTIFICATION BANNER
  if (Object.keys(gridDataObj).length === 0) {
    noDataNotification.classList.remove("is-hidden");
  } else {
    noDataNotification.classList.add("is-hidden");
  }

  // FILL AG GRID WITH DATA
  gridApi.setGridOption("rowData", gridDataObj);
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
  "200-2", //stop timing
  "201-2",
  "182-1", //power failure
  "180-1", //Stop time
  "180-0",
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
  "184-1", //power restored
  "200-1", //power up alarm
  "201-1",
  "200-5",
  "201-5",
  "200-48",
  "201-48",
  "200-73",
  "201-73",
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
    { field: "time_increment", headerName: "Time Increment (sec)" },
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
    let newURL = `${APP_URL}/?startdt=${start_dtInput.value}&enddt=${end_dtInput.value}&locid=${locationSel.value}`;
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
});

// AFTER ALL SCRIPT AND WINDOWS LOADED;
window.onload = async function () {
  // **onload WAITs TO LOAD LOCATIONS IN Locations SELECT ELEMENT
  await fetchLocids();

  console.log("---on load");

  // IF QUERY PARAMETERS PASSED, SET location and datetimes
  // USED for automated links that may be triggered by other apps
  let searchParams = new URLSearchParams(window.location.search);

  if (searchParams.size > 0) {
    //  Set values with .get() method
    locationSel.value = searchParams.get("locid");
    start_dtInput.value = searchParams.get("startdt");
    end_dtInput.value = searchParams.get("enddt");

    fetch_griddata();
  }
};

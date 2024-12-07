import { API_URL } from "./env.js";

const locationSel = document.getElementById("locations");
const start_dtInput = document.getElementById("start_dt");
const end_dtInput = document.getElementById("end_dt");
const addhrsInput = document.getElementById("addhrs");
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

  // default datetime has 'T' format; remove and split date/hour
  let sdt = start_dtInput.value.replace("T", " ");
  let edt = end_dtInput.value.replace("T", " ");

  let response = await fetch(
    `${API_URL}/hiresgrid?locid=${locationSel.value}&startdt=${sdt}&enddt=${edt}`
  );

  // list of dictionaries
  let gridDataObj = await response.json();

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

// Flash codes to color text red
let danger_arr = [173, 180];

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
    { field: "event_descriptor", headerName: "Event Descriptor" },
    { field: "phase_status", headerName: "Phase Status" },
    { field: "ovl_status", headerName: "Overlap Status" },
    { field: "ops_status", headerName: "Operational Status" },
    // { field: "parameter2", headerName: "Parameter 2" },
    // { field: "duration" },
  ],

  autoSizeStrategy: {
    type: "fitCellContents",
    // type: "fitGridWidth",
    // defaultMinWidth: 100,
  },

  rowClassRules: {
    // apply red to Ford cars
    "rag-red": (params) => params.data.event_descriptor.includes("Red"),
    "rag-amber": (params) => params.data.event_descriptor.includes("Yellow"),
    // TODO: only highlight green interval vs split?
    "rag-green": (params) => params.data.event_descriptor.includes("Green"),
    "rag-danger": (params) => danger_arr.includes(params.data.event_code),
  },

  rowSelection: {
    mode: "multiRow",
    checkboxes: false,
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
    await fetch_griddata();
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

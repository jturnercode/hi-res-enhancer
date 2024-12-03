import { API_URL } from "./env.js";

const locationSel = document.getElementById("locations");
const dtimeInput = document.getElementById("dtimeInput");
const addhrsInput = document.getElementById("addhrs");
const getdataBtn = document.getElementById("getdataBtn");
const noDataNotification = document.getElementById("noDataNotification");

/**======================
 *    Set Datetime Input
 * default time
 *========================**/
let cdate = new Date();
dtimeInput.value = `${cdate.getFullYear()}-${
  cdate.getMonth() + 1
}-${cdate.getDate()} 00:00`;

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
  let dt = dtimeInput.value.split("T");

  let response = await fetch(
    `${API_URL}/hiresgrid?locid=${locationSel.value}&date=${
      dt[0]
    }&time=${dt[1].replace(":", "")}&addhrs=${addhrsInput.value}`
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

// const ragCellClassRules = {
//   // apply green to electric cars
//   "rag-green": (params) => params.value === true,
// };

// Grid Options: Contains all of the Data Grid configurations
const gridOptions = {
  // Row Data: The data to be displayed. init w/no data
  rowData: [],

  // Column Definitions: Defines the columns to be displayed.
  columnDefs: [
    { field: "loc_id", headerName: "LocID" },
    { field: "dt", headerName: "Datetime 1" },
    { field: "event_code", headerName: "Event Code 1" },
    { field: "parameter", headerName: "Parameter 1" },
    { field: "event_descriptor", headerName: "Event Descriptor 1" },
    { field: "dt2", headerName: "Datetime 2" },
    { field: "event_code2", headerName: "Event Code 2" },
    { field: "parameter2", headerName: "Parameter 2" },
    { field: "event_descriptor2", headerName: "Event Descriptor 2" },
    { field: "duration" },
  ],

  autoSizeStrategy: {
    type: "fitCellContents",
    // type: "fitGridWidth",
    // defaultMinWidth: 100,
  },

  // TODO: highlight mmu flash, power, etc red with rowclass rule
  // rowClassRules: {
  //   // apply red to Ford cars
  //   "rag-red": (params) => params.data.make === "Ford",
  // },

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
});

// AFTER ALL SCRIPT AND WINDOWS LOADED;
window.onload = async function () {
  // **onload WAITs TO LOAD LOCATIONS IN Locations SELECT ELEMENT
  await fetchLocids();

  // IF QUERY PARAMETERS PASSED (USED FOR LINKS SET PERIOD/LOCAITON)
  let searchParams = new URLSearchParams(window.location.search);
  if (searchParams.size > 0) {
    console.log(searchParams);
    console.log(searchParams.get("locid"));
    console.log(dtimeInput.value);

    locationSel.value = searchParams.get("locid");
    // dtimeInput.value = searchParams.get("date");
    dtimeInput.value = "2024-11-25T02:00";
    addhrsInput.value = 1;
    fetch_griddata();
  }
};

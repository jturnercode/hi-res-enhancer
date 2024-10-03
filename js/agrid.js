const locationSel = document.getElementById("locations");
const dtimeInput = document.getElementById("dtimeInput");
const addhrsInput = document.getElementById("addhrs");
const getdataBtn = document.getElementById("getdataBtn");
const noDataNotification = document.getElementById("noDataNotification");
// const addhrsSel = document.getElementById("addhrs");

/**============================================
 * *     Fetch location dropdown info
 *=============================================**/
async function fetchLocids() {
  let response = await fetch("http://127.0.0.1:8000/form_locids", {
    credentials: "include",
  });
  locObject = await response.json();
  // console.log(locObject);

  // TODO: how do make below code work? worked in sign db
  // for (let loc in locObject) {

  // add data to locations dropdown
  for (const [k, v] of Object.entries(locObject)) {
    // console.log(k, v);
    let newOption = new Option(v.name, v.atms_id);
    locationSel.add(newOption);
  }
}
fetchLocids();

//
// function addHrs() {
//   // add data to addHrs Select
//   for (let i = 1; i <= 32; i++) {
//     let newOption = new Option(i, i);
//     addhrsSel.add(newOption);
//   }
// }
// addHrs();

/**============================================
 *               Fetch grid data function
 *=============================================**/

async function fetch_griddata() {
  // TODO: ***add exception catch, return for ag grid

  let dt = dtimeInput.value.split("T");

  let response = await fetch(
    `http://127.0.0.1:8000/hiresgrid?locid=${locationSel.value}&date=${
      dt[0]
    }&time=${dt[1].replace(":", "")}&addhrs=${addhrsInput.value}`,
    {
      credentials: "include",
    }
  );

  // list of dictionaries
  let gridDataObj = await response.json();

  return gridDataObj;
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
    { field: "dt", headerName: "Datetime" },
    { field: "event_code", headerName: "Event Code 1" },
    { field: "parameter", headerName: "Parameter 1" },
    { field: "event_descriptor", headerName: "Event Descriptor" },
    { field: "dt2", headerName: "Datetime 2" },
    { field: "event_code2", headerName: "Event Code 2" },
    { field: "parameter2", headerName: "Parameter 2" },
    { field: "event_descriptor2", headerName: "Event Descriptor" },
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
   *    Get data Button
   *========================**/
  getdataBtn.addEventListener("click", async function () {
    const f = await fetch_griddata();
    // console.log(f);
    if (Object.keys(f).length === 0) {
      noDataNotification.classList.remove("is-hidden");
    } else {
      noDataNotification.classList.add("is-hidden");
    }
    gridApi.setGridOption("rowData", f);
  });
});

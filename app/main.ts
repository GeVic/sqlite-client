import { open } from "fs/promises";
import { constants } from "fs";
import type { FileHandle } from "fs/promises";
import { sql } from "bun";
import { isUint8Array } from "util/types";

const args = process.argv;
const databaseFilePath: string = args[2];
const command: string = args[3];

type TableInfo = {
  size: number | null;
  name: string | null;
  type: string;
  headerSize: number | null;
  tableName: string;
  rootPage: number | null;
  sql: string | null;
};
// read db file
const databaseFileHandler = await open(databaseFilePath, constants.O_RDONLY);
const buffer: Uint8Array = new Uint8Array(110);
await databaseFileHandler.read(buffer, 0, buffer.length, 0);

const pageSize = new DataView(buffer.buffer, 0, buffer.byteLength).getUint16(
  16,
);
const noOfCells = new DataView(buffer.buffer, 0, buffer.byteLength).getUint16(
  103,
);

const processCells = async (
  numCells: number,
  cellPointerArray: Uint8Array,
  pageOffset: number,
  colIndexes: number[],
  hasIntegerPrimaryKey: boolean = false,
): Promise<string[][]> => {
  const results = [];

  for (let i = 0; i < numCells; i++) {
    let row = [];
    const cellOffset = new DataView(cellPointerArray.buffer).getUint16(i * 2);
    const absOffset = pageOffset + cellOffset;

    const values = await parseCell(
      absOffset,
      databaseFileHandler,
      hasIntegerPrimaryKey,
    );

    // for (let col = 0; col < colIndexes.length; col++) {
    //   if (colIndexes[col] < values.length) {
    //     row.push(values[colIndexes[col]]);
    //   }
    // }
    results.push(values);
  }

  return results;
};

const getColumnIndex = (createSQL: string, columnNames: string[]): number[] => {
  // Extract the column definitions from CREATE TABLE statement
  // Example: "CREATE TABLE apples (id INTEGER, name TEXT, color TEXT)"

  // Find the part between parentheses
  const match = createSQL.match(/\((.*)\)/s);
  if (!match) return [];

  // Get the column definitions
  const columnDefs = match[1];

  // Split by comma (but be careful of commas inside constraints)
  // Simple approach for basic tables:
  const columns = columnDefs.split(",").map((col) => col.trim());
  let columnIndices = [];

  // Extract column names
  for (let col = 0; col < columnNames.length; col++) {
    for (let i = 0; i < columns.length; i++) {
      // Each column definition starts with the column name
      const colName = columns[i].split(/\s+/)[0].toLowerCase();

      if (colName === columnNames[col].toLowerCase()) {
        columnIndices.push(i);
      }
    }
  }
  return columnIndices;
};

const readTableData = async (
  rootPage: number,
  databaseFileHandler: FileHandle,
  columnIndexes: number[],
  hasIntegerPrimaryKey: boolean = false,
): Promise<string[][]> => {
  let offset = (rootPage - 1) * pageSize;

  // Page header
  const pageHeader: Uint8Array = new Uint8Array(8);
  await databaseFileHandler.read(pageHeader, 0, pageHeader.length, offset);

  // no of cells/rows
  const numCells = new DataView(pageHeader.buffer).getUint16(3);

  // cell pointer array
  const cellPtArray: Uint8Array = new Uint8Array(2 * numCells);
  await databaseFileHandler.read(
    cellPtArray,
    0,
    cellPtArray.length,
    offset + 8,
  );

  const results = await processCells(
    numCells,
    cellPtArray,
    offset,
    columnIndexes,
    hasIntegerPrimaryKey,
  );

  return results;
};

const countRowsInTable = async (
  rootPage: number,
  databaseFileHandler: FileHandle,
): Promise<number> => {
  const pageBuffer: Uint8Array = new Uint8Array(8);
  const offset = (rootPage - 1) * pageSize;
  await databaseFileHandler.read(pageBuffer, 0, pageBuffer.length, offset);
  const rows = new DataView(pageBuffer.buffer).getUint16(3);
  return rows;
};

type Varint = { value: number; bytesRead: number };

/** Read a big-endian varint from buf at offset. */
function readVarint(buf: Uint8Array, start: number): Varint {
  let value = 0;
  let bytesRead = 0;

  // Up to 9 bytes per SQLite varint
  for (; bytesRead < 9; bytesRead++) {
    const byte = buf[start + bytesRead];
    if (bytesRead === 8) {
      // 9th byte uses all 8 bits
      value = (value << 8) | byte;
      bytesRead++;
      break;
    }
    value = (value << 7) | (byte & 0x7f);
    if ((byte & 0x80) === 0) {
      // high bit clear → end of varint
      bytesRead++;
      break;
    }
  }

  return { value, bytesRead };
}

const parseCell = async (
  cellOffset: number,
  databaseFileHandler: FileHandle,
  hasIntegerPrimaryKey: boolean = false,
): Promise<any[]> => {
  // Read enough bytes to parse the cell
  const cellData: Uint8Array = new Uint8Array(1000);
  await databaseFileHandler.read(cellData, 0, cellData.length, cellOffset);

  let pos = 0;

  // Parse payload size varint
  const { value: payloadSize, bytesRead: payloadBytes } = readVarint(
    cellData,
    pos,
  );
  pos += payloadBytes;

  // Parse row ID varint
  const { value: rowId, bytesRead: rowIdBytes } = readVarint(cellData, pos);
  pos += rowIdBytes;

  // Parse record header size varint
  const { value: recordHeaderSize, bytesRead: headerBytes } = readVarint(
    cellData,
    pos,
  );
  pos += headerBytes;

  // Read serial types
  const serialTypes: number[] = [];
  const headerEnd = pos + recordHeaderSize - headerBytes;
  while (pos < headerEnd) {
    const { value: serialType, bytesRead } = readVarint(cellData, pos);
    serialTypes.push(serialType);
    pos += bytesRead;
  }

  // Parse all column values
  const values: any[] = [];
  for (const serialType of serialTypes) {
    if (serialType === 0) {
      //values.push(null);
    } else if (serialType === 1) {
      values.push(cellData[pos]);
      pos += 1;
    } else if (serialType === 2) {
      values.push(new DataView(cellData.buffer).getUint16(pos));
      pos += 2;
    } else if (serialType === 3) {
      // 24-bit signed integer
      const val =
        (cellData[pos] << 16) | (cellData[pos + 1] << 8) | cellData[pos + 2];
      values.push(val > 0x7fffff ? val - 0x1000000 : val); // Handle sign
      pos += 3;
    } else if (serialType === 4) {
      values.push(new DataView(cellData.buffer).getUint32(pos));
      pos += 4;
    } else if (serialType === 5) {
      // 48-bit signed integer (simplified - may need BigInt for accuracy)
      let val = 0;
      for (let i = 0; i < 6; i++) {
        val = (val << 8) | cellData[pos + i];
      }
      values.push(val);
      pos += 6;
    } else if (serialType === 6) {
      // 64-bit signed integer
      const high = new DataView(cellData.buffer).getUint32(pos);
      const low = new DataView(cellData.buffer).getUint32(pos + 4);
      values.push(high * 0x100000000 + low); // May need BigInt for large values
      pos += 8;
    } else if (serialType === 7) {
      // 64-bit float
      values.push(new DataView(cellData.buffer).getFloat64(pos));
      pos += 8;
    } else if (serialType === 8) {
      values.push(0);
    } else if (serialType === 9) {
      values.push(1);
    } else if (serialType >= 13 && serialType % 2 === 1) {
      // TEXT
      const textLength = (serialType - 13) / 2;
      values.push(
        new TextDecoder().decode(cellData.slice(pos, pos + textLength)),
      );
      pos += textLength;
    } else if (serialType >= 12 && serialType % 2 === 0) {
      // BLOB
      const blobLength = (serialType - 12) / 2;
      values.push(cellData.slice(pos, pos + blobLength));
      pos += blobLength;
    }
  }
  if (hasIntegerPrimaryKey) return [rowId, ...values];
  return values;
};

const getTableInfo = async (
  pointer: number,
  databaseFileHandler: FileHandle,
  hasIntegerPrimaryKey: boolean = false,
): Promise<[number, string, TableInfo]> => {
  const values = await parseCell(
    pointer,
    databaseFileHandler,
    hasIntegerPrimaryKey,
  );

  // Schema table columns: type, name, tbl_name, rootpage, sql
  const tableInfo: TableInfo = {
    size: null,
    name: values[1],
    type: values[0],
    headerSize: null,
    tableName: values[2] || "", // name column
    rootPage: values[3] || null, // rootpage column
    sql: values[4] || null, // sql column
  };

  // Calculate next pointer position (simplified - you'd need payload size)
  return [pointer + 100, tableInfo.tableName, tableInfo];
};

const getPageType = async (rootPage: number) => {
  const pageOffset = (rootPage - 1) * pageSize;
  const pageType: Uint8Array = new Uint8Array(1);
  await databaseFileHandler.read(pageType, 0, 1, pageOffset);
  return pageType[0];
};

const getCellPointerArray = async (
  numCells: number,
  databaseFileHandler: FileHandle,
): Promise<number[]> => {
  const bufferCells: Uint8Array = new Uint8Array(2 * numCells);
  await databaseFileHandler.read(bufferCells, 0, bufferCells.length, 108);

  let pointerArray = [];

  for (let cell = 0; cell < numCells; cell++) {
    const pointer = new DataView(bufferCells.buffer, cell * 2, 2).getUint16(0);
    pointerArray.push(pointer);
  }
  return pointerArray;
};

const parseInteriorPage = async (pageNumber: number) => {
  const pageOffset = (pageNumber - 1) * pageSize;
  const pageHeader = new Uint8Array(8);
  await databaseFileHandler.read(pageHeader, 0, pageHeader.length, pageOffset);

  // const pageType = pageHeader[0];
  // no of cells
  const numCells = new DataView(pageHeader.buffer).getUint16(3);

  // right most pointer
  const rightMost = new Uint8Array(4);
  await databaseFileHandler.read(
    rightMost,
    0,
    rightMost.length,
    pageOffset + 8,
  );
  const rightPointer = new DataView(rightMost.buffer).getUint32(0);

  const cellPtArray: Uint8Array = new Uint8Array(2 * numCells);
  await databaseFileHandler.read(
    cellPtArray,
    0,
    cellPtArray.length,
    pageOffset + 12,
  );

  const childPages = [];
  // Get child pages
  for (let i = 0; i < numCells; i++) {
    const cellOffset = new DataView(cellPtArray.buffer).getUint16(i * 2);

    // cell content
    const cellData = new Uint8Array(100);
    await databaseFileHandler.read(
      cellData,
      0,
      cellData.length,
      pageOffset + cellOffset,
    );
    const leftChild = new DataView(cellData.buffer).getUint32(0);
    childPages.push(leftChild);
  }
  childPages.push(rightPointer);
  return childPages;
};

const searchByRowIdsHelper = async (
  ids: number[],
  idsIdx: number,
  res: Array<{ rowId: number; values: string[] }>,
  pageNum: number,
  leftVal: number,
  colIndexes: number[],
  hasIntegerPrimaryKey: boolean,
): Promise<{
  res: Array<{ rowId: number; values: string[] }>;
  idsIdx2: number;
}> => {
  const pageType = await getPageType(pageNum);
  let idsIdx2 = idsIdx;

  if (pageType === 0x05) {
    // Interior page
    const pageOffset = (pageNum - 1) * pageSize;
    const pageHeader = new Uint8Array(12);
    await databaseFileHandler.read(
      pageHeader,
      0,
      pageHeader.length,
      pageOffset,
    );

    const numCells = new DataView(pageHeader.buffer).getUint16(3);
    const rightMostPtr = new DataView(pageHeader.buffer).getUint32(8);

    const cellPtArray = new Uint8Array(2 * numCells);
    await databaseFileHandler.read(
      cellPtArray,
      0,
      cellPtArray.length,
      pageOffset + 12,
    );

    let lowestVal = leftVal;
    let rowId = 0;

    for (let i = 0; i < numCells; i++) {
      const cellOffset = new DataView(cellPtArray.buffer).getUint16(i * 2);
      const absOffset = pageOffset + cellOffset;

      // Read left child page
      const leftChildBuffer = new Uint8Array(4);
      await databaseFileHandler.read(
        leftChildBuffer,
        0,
        leftChildBuffer.length,
        absOffset,
      );
      const leftChildPage = new DataView(leftChildBuffer.buffer).getUint32(0);

      // Read rowId key
      const rowIdBuffer = new Uint8Array(9);
      await databaseFileHandler.read(
        rowIdBuffer,
        0,
        rowIdBuffer.length,
        absOffset + 4,
      );
      const { value: rowIdKey } = readVarint(rowIdBuffer, 0);
      rowId = rowIdKey;

      if (
        idsIdx2 < ids.length &&
        ids[idsIdx2] <= rowId &&
        ids[idsIdx2] >= lowestVal
      ) {
        const { idsIdx2: newIdsIdx2 } = await searchByRowIdsHelper(
          ids,
          idsIdx2,
          res,
          leftChildPage,
          lowestVal,
          colIndexes,
          hasIntegerPrimaryKey,
        );
        idsIdx2 = newIdsIdx2;
      }
      lowestVal = rowId;
    }

    if (idsIdx2 < ids.length && ids[idsIdx2] >= rowId) {
      const { idsIdx2: newIdsIdx2 } = await searchByRowIdsHelper(
        ids,
        idsIdx2,
        res,
        rightMostPtr,
        rowId,
        colIndexes,
        hasIntegerPrimaryKey,
      );
      idsIdx2 = newIdsIdx2;
    }
  } else if (pageType === 0x0d) {
    // Leaf page
    const pageOffset = (pageNum - 1) * pageSize;
    const pageHeader = new Uint8Array(8);
    await databaseFileHandler.read(
      pageHeader,
      0,
      pageHeader.length,
      pageOffset,
    );

    const numCells = new DataView(pageHeader.buffer).getUint16(3);
    const cellPtArray = new Uint8Array(2 * numCells);
    await databaseFileHandler.read(
      cellPtArray,
      0,
      cellPtArray.length,
      pageOffset + 8,
    );

    for (let i = 0; i < numCells; i++) {
      const cellOffset = new DataView(cellPtArray.buffer).getUint16(i * 2);
      const absOffset = pageOffset + cellOffset;

      const values = await parseCell(
        absOffset,
        databaseFileHandler,
        hasIntegerPrimaryKey,
      );

      const rowId = hasIntegerPrimaryKey
        ? parseInt(values[0])
        : parseInt(values[0]);

      if (idsIdx2 < ids.length && rowId === ids[idsIdx2]) {
        res.push({
          rowId,
          values: colIndexes.map((idx) => values[idx]),
        });
        idsIdx2++;
      }
    }
  }

  return { res, idsIdx2 };
};

const searchByRowIds = async (
  pageNumber: number,
  targetRowIds: number[],
  startIdx: number,
  colIndexes: number[],
  hasIntegerPrimaryKey: boolean = false,
): Promise<{ results: Map<number, string[]>; nextIdx: number }> => {
  const resArray: Array<{ rowId: number; values: string[] }> = [];
  const { idsIdx2 } = await searchByRowIdsHelper(
    targetRowIds,
    0,
    resArray,
    pageNumber,
    0,
    colIndexes,
    hasIntegerPrimaryKey,
  );

  const results = new Map<number, string[]>();
  for (const item of resArray) {
    results.set(item.rowId, item.values);
  }

  return { results, nextIdx: idsIdx2 };
};

const searchByRowId = async (
  pageNumber: number,
  targetRowId: number,
  colIndexes: number[],
  hasIntegerPrimaryKey: boolean = false,
): Promise<string[] | null> => {
  const pageType = await getPageType(pageNumber);

  if (pageType === 0x0d) {
    // Leaf page - search for specific row
    const pageOffset = (pageNumber - 1) * pageSize;
    const pageHeader: Uint8Array = new Uint8Array(8);
    await databaseFileHandler.read(
      pageHeader,
      0,
      pageHeader.length,
      pageOffset,
    );

    const numCells = new DataView(pageHeader.buffer).getUint16(3);
    const cellPtArray: Uint8Array = new Uint8Array(2 * numCells);
    await databaseFileHandler.read(
      cellPtArray,
      0,
      cellPtArray.length,
      pageOffset + 8,
    );

    // Check each cell
    for (let i = 0; i < numCells; i++) {
      const cellOffset = new DataView(cellPtArray.buffer).getUint16(i * 2);
      const absOffset = pageOffset + cellOffset;

      const values = await parseCell(
        absOffset,
        databaseFileHandler,
        hasIntegerPrimaryKey,
      );

      // Check if this is our target row
      const rowId = hasIntegerPrimaryKey ? values[0] : parseInt(values[0]);
      if (rowId === targetRowId) {
        // Found it! Return just the columns we need
        return colIndexes.map((idx) => values[idx]);
      }
    }
    return null; // Not found in this page
  } else if (pageType === 0x05) {
    // Interior page - check all child pages
    const childPages = await parseInteriorPage(pageNumber);

    for (const childPage of childPages) {
      const result = await searchByRowId(
        childPage,
        targetRowId,
        colIndexes,
        hasIntegerPrimaryKey,
      );
      if (result) {
        return result; // Found in child, return immediately
      }
    }
  }

  return null; // Not found
};

const traverseBtree = async (
  pageNumber: number,
  colIndexes: number[],
  hasIntegerPrimaryKey: boolean = false,
): Promise<string[][]> => {
  const pageType = await getPageType(pageNumber);
  let allResults = [];

  if (pageType === 0x0d) {
    allResults = await readTableData(
      pageNumber,
      databaseFileHandler,
      colIndexes,
      hasIntegerPrimaryKey,
    );

    for (const row in allResults) {
    }
  } else if (pageType === 0x05) {
    const childPages = await parseInteriorPage(pageNumber);

    for (const childPage of childPages) {
      const result = await traverseBtree(
        childPage,
        colIndexes,
        hasIntegerPrimaryKey,
      );
      allResults.push(...result);
    }
  }
  return allResults;
};

const traverseIndexBtreeHelper = async (
  pageNum: number,
  searchValue: string,
  leftVal: string,
  results: number[],
): Promise<void> => {
  const pageOffset = (pageNum - 1) * pageSize;
  const pageHeader = new Uint8Array(8);
  await databaseFileHandler.read(pageHeader, 0, pageHeader.length, pageOffset);

  const noOfCells = new DataView(pageHeader.buffer).getUint16(3);
  const pageType = await getPageType(pageNum);

  if (pageType === 0x02) {
    // interior index page
    const rightPtrBuffer = new Uint8Array(4);
    await databaseFileHandler.read(
      rightPtrBuffer,
      0,
      rightPtrBuffer.length,
      pageOffset + 8,
    );
    const rightMostPtr = new DataView(rightPtrBuffer.buffer).getUint32(0);

    const cellPtrBuffer = new Uint8Array(2 * noOfCells);
    await databaseFileHandler.read(
      cellPtrBuffer,
      0,
      cellPtrBuffer.length,
      pageOffset + 12,
    );

    let lowestVal = leftVal;
    let lastKeyValue = "";
    let lastRowId = 0;

    for (let i = 0; i < noOfCells; i++) {
      const pointer = new DataView(cellPtrBuffer.buffer, i * 2, 2).getUint16(0);
      const cellOffset = pageOffset + pointer;

      const childPageBuffer = new Uint8Array(4);
      await databaseFileHandler.read(
        childPageBuffer,
        0,
        childPageBuffer.length,
        cellOffset,
      );
      const leftChildPage = new DataView(childPageBuffer.buffer).getUint32(0);

      // Read the payload to get the key value
      const payloadBuffer = new Uint8Array(9);
      await databaseFileHandler.read(
        payloadBuffer,
        0,
        payloadBuffer.length,
        cellOffset + 4,
      );
      const { value: payloadSize, bytesRead } = readVarint(payloadBuffer, 0);

      const payload = new Uint8Array(payloadSize);
      await databaseFileHandler.read(
        payload,
        0,
        payload.length,
        cellOffset + 4 + bytesRead,
      );

      let pos = 0;
      const { value: headerSize, bytesRead: headerBytes } = readVarint(
        payload,
        0,
      );
      pos += headerBytes;

      const serialTypes: number[] = [];
      const headerend = headerBytes + headerSize - headerBytes;

      while (pos < headerend) {
        const { value: serialType, bytesRead } = readVarint(payload, pos);
        serialTypes.push(serialType);
        pos += bytesRead;
      }

      let keyValue = "";
      const firstSerialType = serialTypes[0];
      const secondSerialType = serialTypes[1];

      if (firstSerialType >= 13 && firstSerialType % 2 === 1) {
        const textLength = (firstSerialType - 13) / 2;
        keyValue = new TextDecoder().decode(
          payload.slice(pos, pos + textLength),
        );
        pos += textLength;
      }

      let rowId = 0;
      if (secondSerialType === 1) {
        rowId = payload[pos];
      } else if (secondSerialType === 2) {
        rowId = new DataView(payload.buffer).getUint16(pos);
      } else if (secondSerialType === 3) {
        const val =
          (payload[pos] << 16) | (payload[pos + 1] << 8) | payload[pos + 2];
        rowId = val > 0x7fffff ? val - 0x1000000 : val;
      } else if (secondSerialType === 4) {
        rowId = new DataView(payload.buffer).getUint32(pos);
      }

      lastKeyValue = keyValue;
      lastRowId = rowId;

      if (searchValue === keyValue) {
        results.push(rowId);
      }

      if (searchValue <= keyValue && searchValue >= lowestVal) {
        await traverseIndexBtreeHelper(
          leftChildPage,
          searchValue,
          keyValue,
          results,
        );
      }

      lowestVal = keyValue;
    }

    if (searchValue >= lastKeyValue) {
      await traverseIndexBtreeHelper(
        rightMostPtr,
        searchValue,
        lastKeyValue,
        results,
      );
    }
  } else if (pageType === 0x0a) {
    // leaf index page
    const cellPtrBuffer = new Uint8Array(2 * noOfCells);
    await databaseFileHandler.read(
      cellPtrBuffer,
      0,
      cellPtrBuffer.length,
      pageOffset + 8,
    );

    for (let i = 0; i < noOfCells; i++) {
      const pointer = new DataView(cellPtrBuffer.buffer, i * 2, 2).getUint16(0);
      const cellOffset = pageOffset + pointer;

      const payloadSizeBuffer = new Uint8Array(10);
      await databaseFileHandler.read(
        payloadSizeBuffer,
        0,
        payloadSizeBuffer.length,
        cellOffset,
      );

      const { value: payloadSize, bytesRead } = readVarint(
        payloadSizeBuffer,
        0,
      );

      const payload = new Uint8Array(payloadSize);
      await databaseFileHandler.read(
        payload,
        0,
        payload.length,
        cellOffset + bytesRead,
      );

      let pos = 0;
      const { value: headerSize, bytesRead: headerSizeBytes } = readVarint(
        payload,
        pos,
      );
      pos += headerSizeBytes;
      const headerEnd = pos + headerSize - headerSizeBytes;

      let serialTypes: number[] = [];
      while (pos < headerEnd) {
        const { value: serialType, bytesRead } = readVarint(payload, pos);
        serialTypes.push(serialType);
        pos += bytesRead;
      }

      const firstValue = serialTypes[0];
      const secondValue = serialTypes[1];

      let clauseValue = "";
      if (firstValue >= 13 && firstValue % 2 === 1) {
        const textLength = (firstValue - 13) / 2;
        clauseValue = new TextDecoder().decode(
          payload.slice(pos, pos + textLength),
        );
        pos += textLength;
      }

      let rowId = 0;
      if (secondValue === 1) {
        rowId = payload[pos];
      } else if (secondValue === 2) {
        rowId = new DataView(payload.buffer).getUint16(pos);
      } else if (secondValue === 3) {
        const val =
          (payload[pos] << 16) | (payload[pos + 1] << 8) | payload[pos + 2];
        rowId = val > 0x7fffff ? val - 0x1000000 : val;
      } else if (secondValue === 4) {
        rowId = new DataView(payload.buffer).getUint32(pos);
      } else if (secondValue === 5) {
        const high = new DataView(payload.buffer).getUint16(pos);
        const low = new DataView(payload.buffer).getUint32(pos + 2);
        rowId = high * 0x100000000 + low;
      } else if (secondValue === 6) {
        const high = new DataView(payload.buffer).getUint32(pos);
        const low = new DataView(payload.buffer).getUint32(pos + 4);
        rowId = high * 0x100000000 + low;
      }

      if (clauseValue === searchValue) {
        results.push(rowId);
      }
    }
  }
};

const traverseIndexBtree = async (
  rootPage: number,
  searchValue: string,
): Promise<number[]> => {
  const results: number[] = [];
  await traverseIndexBtreeHelper(rootPage, searchValue, "", results);
  return results;
};

switch (command) {
  case ".dbinfo": {
    console.log(`database page size: ${pageSize}`);
    console.log(`number of tables: ${noOfCells}`);

    await databaseFileHandler.close();
    break;
  }
  case ".tables": {
    const cellPointerArray = await getCellPointerArray(
      noOfCells,
      databaseFileHandler,
    );
    let tableNames = [];

    for (let cell = 0; cell < noOfCells; cell++) {
      let [recordPtr, tableName, _] = await getTableInfo(
        cellPointerArray[cell],
        databaseFileHandler,
        false,
      );
      if (!tableName.startsWith("sqlite_")) tableNames.push(tableName);
    }
    console.log(tableNames.join(" "));

    await databaseFileHandler.close();
    break;
  }
  default: {
    if (command.toUpperCase().startsWith(`SELECT COUNT(*) FROM `)) {
      const tblName = command.split(" ").at(-1);
      const cellPointerArray = await getCellPointerArray(
        noOfCells,
        databaseFileHandler,
      );

      for (let cell = 0; cell < noOfCells; cell++) {
        let [, , tableInfo] = await getTableInfo(
          cellPointerArray[cell],
          databaseFileHandler,
          false,
        );
        if (tableInfo.tableName === tblName) {
          const rowCount = await countRowsInTable(
            tableInfo.rootPage as number,
            databaseFileHandler,
          );
          console.log(rowCount);
        }
      }
    } else if (command.toUpperCase().includes("WHERE")) {
      const whereMatch = command.match(/WHERE\s+(\w+)\s*=\s*['"]([^'"]+)['"]/i);
      if (!whereMatch) break;
      const whereClause = {
        column: whereMatch[1],
        value: whereMatch[2],
      };
      const selectPart = command.substring(
        6,
        command.toUpperCase().indexOf(" FROM "),
      );
      const tblColNames = selectPart.split(",").map((col) => col.trim());
      const fromMatch = command.match(/FROM\s+(\w+)/i);
      const tblName = fromMatch ? fromMatch[1] : command.split(" ").at(-1);
      let clauseColIndex: number;

      const cellPointerArray = await getCellPointerArray(
        noOfCells,
        databaseFileHandler,
      );

      let indexRootPage = null;
      let tableRootPage = null;
      let tableSql = null;

      // Find both the index and the table
      for (let cell = 0; cell < noOfCells; cell++) {
        let [_, tableName, tableInfo] = await getTableInfo(
          cellPointerArray[cell],
          databaseFileHandler,
          false,
        );

        // Check for index on the WHERE column
        if (
          tableInfo.type === "index" &&
          tableInfo.tableName === tblName &&
          tableInfo.name?.includes(whereClause.column)
        ) {
          indexRootPage = tableInfo.rootPage;
        }

        // Also get the table information
        if (tableInfo.type === "table" && tableInfo.tableName === tblName) {
          tableRootPage = tableInfo.rootPage;
          tableSql = tableInfo.sql;
        }
      }

      if (indexRootPage && tableRootPage && tableSql) {
        // Use index for fast lookup
        const rowIds: number[] = await traverseIndexBtree(
          indexRootPage,
          whereClause.value,
        );

        const columnIndexes = getColumnIndex(tableSql, tblColNames);
        const hasIntegerPrimaryKey = tableSql.includes("integer primary key");

        // Sort rowIds for efficient traversal
        rowIds.sort((a, b) => a - b);
        const sortedRowIds = rowIds;

        // Fetch all rows in a single traversal
        const { results: rowsMap } = await searchByRowIds(
          tableRootPage,
          sortedRowIds,
          0,
          columnIndexes,
          hasIntegerPrimaryKey,
        );

        // Output rows in the original order they were found in the index
        for (const rowId of rowIds) {
          const row = rowsMap.get(rowId);
          if (row) {
            console.log(row.join("|"));
          }
        }
      } else if (tableRootPage && tableSql) {
        // No index found - fall back to full table scan
        const clauseColIndex = getColumnIndex(tableSql, [
          whereClause.column,
        ])[0];
        const columnIndexes = getColumnIndex(tableSql, tblColNames);
        const hasIntegerPrimaryKey = tableSql.includes("integer primary key");

        const results = await traverseBtree(
          tableRootPage,
          columnIndexes,
          hasIntegerPrimaryKey,
        );

        // Filter results by WHERE clause
        const filteredResults = results.filter((row) => {
          return row[clauseColIndex] === whereClause.value;
        });

        for (const row of filteredResults) {
          const selectedCols = columnIndexes.map((idx) => row[idx]);
          console.log(selectedCols.join("|"));
        }
      }
      break;
    } else if (command.toUpperCase().startsWith(`SELECT`)) {
      const selectPart = command.substring(
        6,
        command.toUpperCase().indexOf(" FROM "),
      );
      const tblColNames = selectPart.split(",").map((col) => col.trim());
      const tblName = command.split(" ").at(-1);
      const cellPointerArray = await getCellPointerArray(
        noOfCells,
        databaseFileHandler,
      );

      for (let cell = 0; cell < noOfCells; cell++) {
        let [_, tableName, tableInfo] = await getTableInfo(
          cellPointerArray[cell],
          databaseFileHandler,
          false,
        );

        if (tableInfo.tableName === tblName) {
          const hasIntegerPrimaryKey = tableInfo.sql!.includes(
            "integer primary key",
          );
          const columnIndexes = getColumnIndex(tableInfo.sql!, tblColNames);
          const results = await readTableData(
            tableInfo.rootPage!,
            databaseFileHandler,
            columnIndexes,
            hasIntegerPrimaryKey,
          );
          const finalResults = results.map((row) =>
            columnIndexes.map((index) => row[index]),
          );
          console.log(finalResults.map((row) => row.join("|")).join("\n"));
        }
      }
      break;
    } else {
      throw new Error(`Unknown command ${command}`);
    }
    await databaseFileHandler.close();
    break;
  }
}

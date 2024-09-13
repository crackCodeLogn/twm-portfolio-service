package com.vv.personal.twm.portfolio.model;

import com.vv.personal.twm.artifactory.generated.equitiesMarket.MarketDataProto;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Vivek
 * @since 2024-09-11
 *     <p>Hold a linked list of DataNode
 */
@Slf4j
@Getter
public class DataList {

  private DataNode head;
  private DataNode tail;

  public DataList() {
    head = null;
    tail = null;
  }

  public void addBlock(MarketDataProto.Instrument instrument) {
    DataNode dataNode = new DataNode(instrument);
    if (head == null) {
      head = dataNode;
      tail = dataNode;
    } else {
      if (instrument.getDirection() == MarketDataProto.Direction.BUY) {
        dataNode.setPrev(tail);
        tail.setNext(dataNode);
        tail = dataNode;
      } else { // in case of SELL, need to find the correct place for new node according to trade
        // date
        boolean insertedSellNode = false;
        DataNode last = tail;
        while (last != null) { // as the first node is assumed to be of BUY type, ALWAYS.
          // keep going left until a date lower than sell is found and insert after that

          if (isCorrectPlaceForSell(instrument, last.getInstrument())) {
            dataNode.setNext(last.getNext());
            dataNode.setPrev(last);
            last.getNext().setPrev(dataNode);
            last.setNext(dataNode);
            insertedSellNode = true;
            break;
          }

          last = last.getPrev();
        }

        if (!insertedSellNode) {
          throw new UnsupportedOperationException(
              "Does not allow short selling of instruments yet!");
        }
      }
    }

    // dataNode.computeAcb(); // removing from here due to possible middle placement of sell blocks
    // causing lot of compute overhead
  }

  /** Invoke at end of construction of DataList, in order to compute ACB for all nodes of list */
  public void computeAcb() {
    DataNode current = head;
    while (current != null) {
      current.computeAcb();
      current = current.getNext();
    }
  }

  public void display() {
    DataNode current = head;
    while (current != null) {
      log.info("{}", current);
      current = current.getNext();
    }
  }

  private boolean isCorrectPlaceForSell(
      MarketDataProto.Instrument sellInstrument, MarketDataProto.Instrument instrument) {
    return sellInstrument.getTicker().getData(0).getDate()
        > instrument.getTicker().getData(0).getDate();
  }

  DataNode getHead() { // only allowed for testing
    return head;
  }
}

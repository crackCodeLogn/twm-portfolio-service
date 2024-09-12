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
    } else {
      dataNode.setPrev(tail);
      tail.setNext(dataNode);
    }
    tail = dataNode;
    dataNode.computeAcb(); // computing acb once linking is done
  }

  public void display() {
    DataNode current = head;
    while (current != null) {
      log.info("{}", current);
      current = current.getNext();
    }
  }
}

package com.vv.personal.twm.portfolio.remote.feign;

import static com.vv.personal.twm.portfolio.constants.GlobalConstants.CALC_SERVICE_FEIGN_NAME;

import com.vv.personal.twm.artifactory.generated.data.DataPacketProto;
import com.vv.personal.twm.artifactory.generated.deposit.FixedDepositProto;
import com.vv.personal.twm.ping.remote.feign.PingFeign;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

/**
 * @author Vivek
 * @since 2025-01-04
 */
@FeignClient(CALC_SERVICE_FEIGN_NAME)
public interface CalcServiceFeign extends PingFeign {

  @PostMapping("/calc/bank/fd/amounts")
  DataPacketProto.DataPacket getFixedDepositAmounts(
      @RequestBody FixedDepositProto.FixedDepositList fixedDepositList);

  @PostMapping("/calc/bank/fd/amount")
  DataPacketProto.DataPacket getFixedDepositAmount(
      @RequestBody FixedDepositProto.FixedDeposit fixedDeposit);
}

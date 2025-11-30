package com.vv.personal.twm.portfolio.remote.controller;

import com.vv.personal.twm.portfolio.service.ProgressTrackerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

/**
 * @author Vivek
 * @since 2025-11-30
 */
@Slf4j
@RestController("progress")
@Controller
@RequestMapping("/progress/")
@CrossOrigin(origins = "http://localhost:5173") // Allow React app
@RequiredArgsConstructor
public class ProgressTrackerController {
  private final ProgressTrackerService progressTrackerService;

  @GetMapping("query")
  public String queryProgressTracker(@RequestParam("client") String clientId) {
    log.info("progress requested for {}", clientId);
    String progress = progressTrackerService.getProgressTracker(clientId).name();
    log.info("progress status for {} => {}", clientId, progress);
    return progress;
  }
}

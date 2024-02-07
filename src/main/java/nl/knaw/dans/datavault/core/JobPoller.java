/*
 * Copyright (C) 2024 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.datavault.core;

import io.dropwizard.hibernate.UnitOfWork;
import io.dropwizard.lifecycle.Managed;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.datavault.db.JobDao;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class JobPoller implements Runnable, Managed {

    private final JobDao jobDao;

    private final JobHandler jobHandler;

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private boolean running;

    public JobPoller(@NonNull JobDao jobDao, @NonNull JobHandler jobHandler) {
        this.jobDao = jobDao;
        this.jobHandler = jobHandler;
    }

    @Override
    public void run() {
        sleep(20000);
        while (running) {
            pollForJobs();
            sleep(1000); // TODO: make configurable
        }
    }

    private void pollForJobs() {
        Optional<Job> nextJob = jobDao.getNextJob();
        nextJob.ifPresent(job -> {
            log.info("Processing job {}", job.getId());
            jobHandler.handleJob(job);
        });
    }

    @SneakyThrows
    private void sleep(int i) {
        Thread.sleep(i);
    }

    @Override
    public void start() throws Exception {
        log.info("Starting JobPoller");
        running = true;
        executorService.execute(this);
    }

    @Override
    public void stop() throws Exception {
        log.info("Stopping JobPoller");
        running = false;
        executorService.shutdown();
    }
}
require('dotenv').config(); 
const admin = require('firebase-admin');
const serviceAccount = require(process.env.GOOGLE_APPLICATION_CREDENTIALS);

// Initialize Firebase Admin SDK
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "https://handzy-c04d2-default-rtdb.firebaseio.com",
});

const db = admin.database();
const JOBS_REF = '/jobs';
const WORKERS_REF = '/workers';
const PAST_JOBS_REF = '/PastJobs';

/**
 * Notify a specific worker about a job.
 * @param {Object} worker - Worker details.
 * @param {string} jobId - Job ID.
 * @param {Object} jobData - Job data.
 */
async function notifyWorker(worker, jobId, jobData) {
  console.log(`Notifying worker ${worker.uid} about job ${jobId}`);
  try {
    await db.ref(`${WORKERS_REF}/${worker.uid}/jobNotification`).set({
      jobId: jobId,
      status: 'pending',
      jobDetails: jobData,
    });
  } catch (error) {
    console.error(`Error notifying worker ${worker.uid}:`, error);
  }
}

/**
 * Check if a job is expired (older than 12 hours)
 * @param {string} timestamp - Job timestamp
 * @returns {boolean} - True if job is expired
 */
function isJobExpired(timestamp) {
  if (!timestamp) return false;
  const now = new Date();
  const jobTime = new Date(timestamp);
  const hoursDifference = (now - jobTime) / (1000 * 60 * 60);
  return hoursDifference >= 12;
}

/**
 * Move expired job to past jobs.
 * @param {string} jobId - Job ID.
 * @param {Object} jobData - Job data.
 */
async function moveToPastJobs(jobId, jobData) {
  console.log(`Job ${jobId} is expired (older than 12 hours). Moving to past jobs.`);
  await db.ref(`${PAST_JOBS_REF}/${jobId}`).set({
    ...jobData,
    status: 'no worker accepted'
  });
  await db.ref(`${JOBS_REF}/${jobId}`).remove();
}

/**
 * Process workers for a specific job.
 */
async function processWorkers(jobId, jobData) {
  const workersSnapshot = await db.ref(WORKERS_REF).orderByChild('availability').equalTo('available').once('value');
  if (!workersSnapshot.exists()) {
    console.log(`No available workers for job: ${jobId}`);
    await db.ref(`${JOBS_REF}/${jobId}`).update({ status: 'pending' });
    return;
  }

  const workers = [];
  workersSnapshot.forEach((workerSnap) => {
    const workerData = workerSnap.val();
    if (workerData.service === jobData.job) {
      workers.push({ ...workerData, uid: workerSnap.key });
    }
  });

  if (workers.length === 0) {
    console.log(`No available workers with matching service for job: ${jobId}`);
    await db.ref(`${JOBS_REF}/${jobId}`).update({ status: 'pending' });
    return;
  }

  workers.sort((a, b) => a.distance - b.distance); // Sort workers by distance

  // Process workers in parallel (with proper handling of async tasks)
  const workerProcessingPromises = workers.map(worker => handleWorkerNotification(worker, jobId, jobData));

  await Promise.all(workerProcessingPromises); // Run all notifications for workers in parallel
}

/**
 * Handle worker notification, response, and job assignment.
 */
async function handleWorkerNotification(worker, jobId, jobData) {
  console.log(`Notifying worker: ${worker.uid}`);
  await notifyWorker(worker, jobId, jobData);
  const accepted = await waitForWorkerResponse(worker);

  if (accepted) {
    await assignJobToWorker(worker, jobId);
  } else {
    console.log(`Worker ${worker.uid} declined the job.`);
    await db.ref(`${WORKERS_REF}/${worker.uid}/jobNotification`).remove();
  }
}

/**
 * Simulate waiting for a worker's response.
 * @param {Object} worker - Worker details.
 * @returns {Promise<boolean>} - True if accepted, otherwise false.
 */
function waitForWorkerResponse(worker) {
  return new Promise((resolve) => {
    setTimeout(() => {
      const randomResponse = Math.random() > 0.5; // Simulated response (50% chance)
      resolve(randomResponse);
    }, 15000); // Wait 15 seconds
  });
}

/**
 * Assign a job to a worker using a Firebase transaction.
 */
async function assignJobToWorker(worker, jobId) {
  const jobRef = db.ref(`${JOBS_REF}/${jobId}`);
  const transactionResult = await jobRef.transaction((currentData) => {
    if (currentData && currentData.status === 'pending' && !isJobExpired(currentData.timestamp)) {
      return {
        ...currentData,
        status: 'assigned',
        worker_id: worker.uid,
      };
    }
    return; // Abort transaction if job is no longer pending or is expired
  });

  if (transactionResult.committed) {
    console.log(`Job ${jobId} successfully assigned to worker ${worker.uid}`);
    await db.ref(`${WORKERS_REF}/${worker.uid}`).update({ availability: 'not available' });
    await clearOtherWorkersNotifications(worker.uid, jobId);
  } else {
    console.log(`Job ${jobId} already assigned or expired.`);
  }
}

/**
 * Clear notifications for all other workers once a job is assigned.
 */
async function clearOtherWorkersNotifications(workerUid, jobId) {
  const workersSnapshot = await db.ref(WORKERS_REF).once('value');
  workersSnapshot.forEach((workerSnap) => {
    const workerData = workerSnap.val();
    if (workerSnap.key !== workerUid) {
      db.ref(`${WORKERS_REF}/${workerSnap.key}/jobNotification`).remove();
    }
  });
}

/**
 * Periodically process new jobs.
 */
async function processNewJobs() {
  const startTime = Date.now(); // Record the start time

  const jobsSnapshot = await db.ref(JOBS_REF).orderByChild('status').equalTo('pending').once('value');
  if (!jobsSnapshot.exists()) {
    console.log("No pending jobs to process.");
    return;
  }

  console.log("Jobs Snapshot: ", jobsSnapshot.val());

  const jobs = Object.entries(jobsSnapshot.val());

  const workerProcessingPromises = jobs.map(async ([jobId, jobData]) => {
    if (isJobExpired(jobData.timestamp)) {
      await moveToPastJobs(jobId, jobData);
      return;
    }

    await processWorkers(jobId, jobData);
  });

  await Promise.all(workerProcessingPromises); // Parallelize worker processing

  const endTime = Date.now(); // Record the end time
  const elapsedTime = endTime - startTime; // Calculate the time taken
  console.log(`Processed jobs in ${elapsedTime}ms`);
}

// Periodically process new jobs
setInterval(processNewJobs, 10000); // Run every 10 seconds
console.log("Current Server Time:", new Date().toISOString());

/*
 * ceph-osd engine
 *
 * IO engine using Ceph's libosd to test the OSD daemon.
 *
 */

#include <ceph_osd.h>
#include <semaphore.h>

#include "../fio.h"

struct cephosd_iou {
	struct io_u *io_u;
	int io_complete;
};

struct cephosd_data {
	struct io_u **aio_events;
	struct libosd *osd;
	unsigned char volume[16];
	sem_t active;
};

struct cephosd_options {
	struct thread_data *td;
	int id;
	const char *config;
	const char *cluster;
	const char *volume;
};


static struct fio_option options[] = {
	{
	 .name     = "ceph_osd_id",
	 .lname    = "ceph osd id",
	 .type     = FIO_OPT_INT,
	 .help     = "Integer id of the ceph osd instance",
	 .off1     = offsetof(struct cephosd_options, id),
	 .category = FIO_OPT_C_ENGINE,
	 .group    = FIO_OPT_G_CEPHOSD,
	 },
	{
	 .name     = "ceph_conf",
	 .lname    = "ceph configuration file",
	 .type     = FIO_OPT_STR_STORE,
	 .help     = "Path to ceph.conf",
	 .off1     = offsetof(struct cephosd_options, config),
	 .category = FIO_OPT_C_ENGINE,
	 .group    = FIO_OPT_G_CEPHOSD,
	 },
	{
	 .name     = "ceph_cluster",
	 .lname    = "ceph cluster name",
	 .type     = FIO_OPT_STR_STORE,
	 .help     = "Name of ceph cluster (default=ceph)",
	 .off1     = offsetof(struct cephosd_options, cluster),
	 .category = FIO_OPT_C_ENGINE,
	 .group    = FIO_OPT_G_CEPHOSD,
	 },
	{
	 .name     = "ceph_volume",
	 .lname    = "ceph volume name",
	 .type     = FIO_OPT_STR_STORE,
	 .help     = "Name of ceph volume",
	 .off1     = offsetof(struct cephosd_options, volume),
	 .category = FIO_OPT_C_ENGINE,
	 .group    = FIO_OPT_G_CEPHOSD,
	 },
	{
	 .name = NULL,
	 },
};

static int setup_cephosd_data(struct thread_data *td,
			      struct cephosd_data **cephosd_data_ptr)
{
	struct cephosd_data *data;

	if (td->io_ops->data)
		return 0;

	data = calloc(1, sizeof(struct cephosd_data));
	if (!data)
		goto failed;

	data->aio_events = calloc(td->o.iodepth, sizeof(struct io_u *));
	if (!data->aio_events)
		goto failed;

	sem_init(&data->active, 0, 0);

	*cephosd_data_ptr = data;
	return 0;

failed:
	if (data)
		free(data);
	return 1;

}

static struct io_u *fio_cephosd_event(struct thread_data *td, int event)
{
	struct cephosd_data *data = td->io_ops->data;

	return data->aio_events[event];
}

static int fio_cephosd_getevents(struct thread_data *td, unsigned int min,
				 unsigned int max, const struct timespec *t)
{
	struct cephosd_data *data = td->io_ops->data;
	unsigned int events = 0;
	struct io_u *io_u;
	int i;
	struct cephosd_iou *fov;

	do {
		io_u_qiter(&td->io_u_all, io_u, i) {
			if (!(io_u->flags & IO_U_F_FLIGHT))
				continue;

			fov = (struct cephosd_iou *)io_u->engine_data;
			if (fov->io_complete) {
				fov->io_complete = 0;
				data->aio_events[events] = io_u;
				events++;
			}
		}
		if (events >= min)
			break;
		usleep(100);
	} while (1);

	return events;
}

void cephosd_on_io_completion(int result, uint64_t length, int flags, void *user)
{
	struct io_u *io_u = (struct io_u *)user;
	struct cephosd_iou *iou = (struct cephosd_iou *)io_u->engine_data;
	iou->io_complete = 1;
}

static int fio_cephosd_queue(struct thread_data *td, struct io_u *iou)
{
	struct cephosd_data *data = td->io_ops->data;
	const char *object = iou->file->file_name;
	int r;

	fio_ro_check(td, iou);

	if (iou->ddir == DDIR_WRITE) {
		r = libosd_write(data->osd, object, data->volume, iou->offset,
				 iou->buflen, iou->buf, LIBOSD_WRITE_CB_STABLE,
				 cephosd_on_io_completion, iou);
		if (r != 0) {
			log_err("libosd_write failed with %d\n", r);
			goto failed;
		}
	} else if (iou->ddir == DDIR_READ) {
		r = libosd_read(data->osd, object, data->volume, iou->offset,
				iou->buflen, iou->buf, LIBOSD_READ_FLAGS_NONE,
				cephosd_on_io_completion, iou);
		if (r != 0) {
			log_err("libosd_read failed with %d\n", r);
			goto failed;
		}
#if 0
	} else if (io_u->ddir == DDIR_SYNC) { // TODO: sync
#endif
	} else {
		dprint(FD_IO, "%s: Warning: unhandled ddir: %d\n", __func__,
		       iou->ddir);
		return FIO_Q_COMPLETED;
	}

	return FIO_Q_QUEUED;

failed:
	iou->error = r;
	td_verror(td, iou->error, "xfer");
	return FIO_Q_COMPLETED;
}

static int fio_cephosd_init(struct thread_data *td)
{
	dprint(FD_IO, "fio_cephosd_init\n");
	return 0;
}

static void fio_cephosd_cleanup(struct thread_data *td)
{
	struct cephosd_data *data = td->io_ops->data;
	dprint(FD_IO, "fio_cephosd_cleanup\n");
	if (data) {
		if (data->osd) {
			libosd_shutdown(data->osd);
			libosd_join(data->osd);
			libosd_cleanup(data->osd);
		}

		free(data->aio_events);
		free(data);
	}
}

/* libceph-osd callback functions */
void cephosd_on_active(struct libosd *osd, void *user)
{
	struct cephosd_data *data = (struct cephosd_data*)user;
	sem_post(&data->active);
}
void cephosd_on_shutdown(struct libosd *osd, void *user)
{
	log_err("cephosd_on_shutdown: osd shutting down!\n");
}

struct libosd_callbacks cephosd_callbacks = {
	.osd_active = cephosd_on_active,
	.osd_shutdown = cephosd_on_shutdown,
};

static int fio_cephosd_setup(struct thread_data *td)
{
	struct cephosd_options *o = td->eo;
	struct cephosd_data *data = NULL;
	struct libosd_init_args init_args = {
		.id = o->id,
		.config = o->config,
		.callbacks = &cephosd_callbacks,
	};
	struct fio_file *f;
	unsigned int i;
	int r = 0;

	dprint(FD_IO, "fio_cephosd_setup\n");

	/* allocate engine specific structure to deal with libceph-osd. */
	r = setup_cephosd_data(td, &data);
	if (r) {
		log_err("setup_cephosd_data failed\n");
		goto out;
	}
	td->io_ops->data = data;
	init_args.user = data;

	/* libceph-osd does not allow us to run first in the main thread and
	 * later in a fork child. It needs to be the same process context all
	 * the time. */
	td->o.use_thread = 1;

	/* initialize and start the osd */
	data->osd = libosd_init(&init_args);
	if (data->osd == NULL) {
		log_err("libosd_init failed\n");
		r = -1;
		goto out_cleanup;
	}

	/* query the volume name */
	r = libosd_get_volume(data->osd, o->volume, data->volume);
	if (r != 0) {
		log_err("libosd_get_volume(%s) failed\n", o->volume);
		goto out_shutdown;
	}

	dprint(FD_IO, "fio_cephosd_setup: waiting for active\n");
	sem_wait(&data->active);
	dprint(FD_IO, "fio_cephosd_setup: osd active\n");


	for_each_file(td, f, i) {
		f->real_file_size = td->o.size / td->o.nr_files;
		r = libosd_truncate(data->osd, f->file_name, data->volume,
				    f->real_file_size, LIBOSD_WRITE_CB_UNSTABLE,
				    NULL, NULL);
		if (r != 0) {
			log_err("libosd_truncate(%s) failed with %d\n",
					f->file_name, r);
			goto out_shutdown;
		}
		// TODO: wait for completion, so we don't race later
	}
	return 0;

out_shutdown:
	libosd_shutdown(data->osd);
	dprint(FD_IO, "fio_cephosd_setup: waiting for shutdown\n");
	libosd_join(data->osd);
	dprint(FD_IO, "fio_cephosd_setup: osd shutdown\n");
out_cleanup:
	fio_cephosd_cleanup(td);
out:
	return r;
}

static int fio_cephosd_open(struct thread_data *td, struct fio_file *f)
{
	return 0;
}

static int fio_cephosd_invalidate(struct thread_data *td, struct fio_file *f)
{
	return 0;
}

static void fio_cephosd_io_u_free(struct thread_data *td, struct io_u *io_u)
{
	struct cephosd_iou *o = io_u->engine_data;
	if (o) {
		io_u->engine_data = NULL;
		free(o);
	}
}

static int fio_cephosd_io_u_init(struct thread_data *td, struct io_u *io_u)
{
	struct cephosd_iou *o = malloc(sizeof(*o));
	o->io_complete = 0;
	o->io_u = io_u;
	io_u->engine_data = o;
	return 0;
}

static struct ioengine_ops ioengine = {
	.name			= "ceph-osd",
	.version		= FIO_IOOPS_VERSION,
	.setup			= fio_cephosd_setup,
	.init			= fio_cephosd_init,
	.queue			= fio_cephosd_queue,
	.getevents		= fio_cephosd_getevents,
	.event			= fio_cephosd_event,
	.cleanup		= fio_cephosd_cleanup,
	.open_file		= fio_cephosd_open,
	.invalidate		= fio_cephosd_invalidate,
	.options		= options,
	.io_u_init		= fio_cephosd_io_u_init,
	.io_u_free		= fio_cephosd_io_u_free,
	.option_struct_size	= sizeof(struct cephosd_options),
};

static void fio_init fio_cephosd_register(void)
{
	register_ioengine(&ioengine);
}

static void fio_exit fio_cephosd_unregister(void)
{
	unregister_ioengine(&ioengine);
}

from __future__ import annotations

import enum

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

from .base import Base, EnumType

__all__ = (
    'PipelineModule',
    'PipelineModuleVersion',
    'PipelineModuleTag',
    'PipelineModuleInput',
    'PipelineModuleOutput',
    'PipelineTemplate',
    'PipelineTemplateVersion',
    'PipelineTemplateTask',
    'PipelineTemplateTaskInput',
    'PipelineTemplateTaskOutput',
    'PipelineTemplateTaskLink',
    'Pipeline',
    'PipelineTaskInput',
    'PipelineTaskOutput',
    'PipelineTaskLink',
)


class PipelineModuleTypes(enum.Enum):
    COMPUTE_SESSION = 'COMPUTE_SESSION'
    MANAGER_TASK = 'MANAGER_TASK'


class PipelineStatus(enum.Enum):
    """
    By default, a pipeline is created as RUNNING.
    The scheduler checks dependencies of pipeline tasks and let them be scheduled
    if all their dependencies are successfully finished via predicate checks.

    If it is deactivated, it goes to PENDING.
    Running tasks continue but no new tasks are started.
    A pipeline with a reserved starting time is initially created as PENDING.

    If activated again, it comes back to RUNNING.

    When all tasks are successfully finished, it goes to FINISHED.

    When there are one or more failed tasks, it goes to ERROR.
    The manager no longer starts a new task while keeping running tasks to continue.
    (This is same to PENDING, but the status indicates that there are failed tasks.)

    If the user or admin explicitly terminates the pipeline, it goes to CANCELLED,
    all the remaining tasks are also marked as cancelled, and all running tasks
    are forcibly terminated immediately.
    """

    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    FINISHED = 'FINISHED'
    ERROR = 'ERROR'
    CANCELLED = 'CANCELLED'


class PipelineTaskStatus(enum.Enum):
    """
    By default, a pipeline task is created as PENDING.

    When the associated compute session or a manager's background task gets started,
    it goes to RUNNING.

    When the task finishes, it goes to either SUCCESS or FAILED depending on
    the exit code of the main process.
    Setting FAILED also makes the pipeline to halt by setting it ERROR as well.

    When there is any error in the Backend.AI-side, it goes to ERROR and
    makes the pipeline to halt by setting it ERROR as well.

    When the task is cancelled (either before or after started), it goes to CANCELLED.
    If there is an associated manager bgtask or comptue session that is running,
    it is forcibly terminated immeidately.
    """

    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    ERROR = 'ERROR'
    CANCELLED = 'CANCELLED'


pipeline_module_tag_association = sa.Table(
    "pipeline_module_tag_association",
    Base.metadata,
    sa.Column("module_id", sa.ForeignKey("pipeline_module.id", ondelete="CASCADE"), primary_key=True),
    sa.Column("tag_name", sa.ForeignKey("pipeline_module_tag.name", ondelete="CASCADE", onupdate="CASCADE"), primary_key=True),
)


class PipelineModule(Base):
    __tablename__ = "pipeline_module"

    id = sa.Column(UUID(as_uuid=True), primary_key=True)
    name = sa.Column(sa.String(length=128))
    icon = sa.Column(sa.String(length=256))
    type = sa.Column(EnumType(PipelineModuleTypes))
    created_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    modified_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    domain_name = sa.Column(sa.ForeignKey("domains.name"))
    group_id = sa.Column(sa.ForeignKey("groups.id"))
    user_uuid = sa.Column(sa.ForeignKey("users.uuid"))
    versions = relationship("PipelineModuleVersion")
    tags = relationship("PipelineModuleTag", secondary=pipeline_module_tag_association, back_populates="modules")
    input_links = relationship("PipelineModuleInput")
    output_links = relationship("PipelineModuleOutput")


class PipelineModuleTag(Base):
    __tablename__ = "pipeline_module_tag"

    name = sa.Column(sa.String(length=64), primary_key=True)
    modules = relationship("PipelineModule", secondary=pipeline_module_tag_association, back_populates="tags")


class PipelineModuleInput(Base):
    __tablename__ = "pipeline_module_input"

    module_id = sa.Column(sa.ForeignKey("pipeline_module.id", ondelete="CASCADE"), primary_key=True)
    module = relationship("PpipelineModule", back_populates="input_links")
    name = sa.Column(sa.String(length=64), primary_key=True)


class PipelineModuleOutput(Base):
    __tablename__ = "pipeline_module_output"

    module_id = sa.Column(sa.ForeignKey("pipeline_module.id", ondelete="CASCADE"), primary_key=True)
    module = relationship("PpipelineModule", back_populates="output_links")
    name = sa.Column(sa.String(length=64), primary_key=True)


class PipelineModuleVersion(Base):
    __tablename__ = "pipeline_module_version"

    id = sa.Column(UUID(as_uuid=True), primary_key=True)
    module_id = sa.Column(sa.ForeignKey("pipeline_module.id", ondelete="CASCADE"))
    module = relationship("PipelineModule", back_populates="versions")
    version = sa.Column(sa.Integer)
    created_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    task_spec = sa.Column(JSONB())


class PipelineTemplate(Base):
    __tablename__ = "pipeline_template"

    id = sa.Column(UUID(as_uuid=True), primary_key=True)
    name = sa.Column(sa.String(length=128))
    domain_name = sa.Column(sa.ForeignKey("domains.name"))
    group_id = sa.Column(sa.ForeignKey("groups.id"))
    user_uuid = sa.Column(sa.ForeignKey("users.uuid"))
    created_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    modified_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    versions = relationship("PipelineTemplateVersion")


class PipelineTemplateVersion(Base):
    __tablename__ = "pipeline_template_version"

    id = sa.Column(UUID(as_uuid=True), primary_key=True)
    template_id = sa.Column(sa.ForeignKey("pipeline_template.id"))
    template = relationship("PipelineTemplate", back_populates="versions")


class PipelineTemplateTask(Base):
    __tablename__ = "pipeline_template_task"

    id = sa.Column(UUID(as_uuid=True), primary_key=True)
    name = sa.Column(sa.String(length=128))
    icon = sa.Column(sa.String(length=256))
    type = sa.Column(EnumType(PipelineModuleTypes))
    task_spec = sa.Column(JSONB())
    template_id = sa.Column(sa.ForeignKey("pipeline_template.id", ondelete="CASCADE"))
    template_vid = sa.Column(sa.ForeignKey("pipeline_template_version.id", ondelete="CASCADE"))
    module_id = sa.Column(sa.ForeignKey("pipeline_module.id", ondelete="SET NULL"), nullable=True)
    module_vid = sa.Column(sa.ForeignKey("pipeline_module_version.id", ondelete="SET NULL"), nullable=True)
    layout_coord_x = sa.Column(sa.Integer())
    layout_coord_y = sa.Column(sa.Integer())
    layout_width = sa.Column(sa.Integer())
    layout_height = sa.Column(sa.Integer())
    input_links = relationship("PipelineTemplateTaskInput")
    output_links = relationship("PipelineTemplateTaskOutput")


class PipelineTemplateTaskInput(Base):
    __tablename__ = "pipeline_template_task_input"

    module_id = sa.Column(sa.ForeignKey("pipeline_template_task.id", ondelete="CASCADE"), primary_key=True)
    module = relationship("PpipelineTemplateTask", back_populates="input_links")
    name = sa.Column(sa.String(length=64), primary_key=True)


class PipelineTemplateTaskOutput(Base):
    __tablename__ = "pipeline_template_task_output"

    module_id = sa.Column(sa.ForeignKey("pipeline_template_task.id", ondelete="CASCADE"), primary_key=True)
    module = relationship("PpipelineTemplateTask", back_populates="output_links")
    name = sa.Column(sa.String(length=64), primary_key=True)


class PipelineTemplateTaskLink(Base):
    __tablename__ = "pipeline_template_task_link"

    template_vid = sa.Column(sa.ForeignKey("pipeline_template_version.id", ondelete="CASCADE"), primary_key=True)
    task_output_id = sa.Column(sa.ForeignKey("pipeline_template_task_output.id", ondelete="CASCADE"), primary_key=True)
    task_input_id = sa.Column(sa.ForeignKey("pipeline_template_task_input.id", ondelete="CASCADE"), primary_key=True)


class Pipeline(Base):
    __tablename__ = "pipeline"

    id = sa.Column(UUID(as_uuid=True), primary_key=True)
    name = sa.Column(sa.String(length=128))
    status = sa.Column(EnumType(PipelineStatus))
    status_info = sa.Column(sa.String(length=64))
    template_id = sa.Column(sa.ForeignKey("pipeline_template.id", ondelete="SET NULL"), nullable=True)
    template_vid = sa.Column(sa.ForeignKey("pipeline_template_version.id", ondelete="SET NULL"), nullable=True)
    domain_name = sa.Column(sa.ForeignKey("domains.name"))
    group_id = sa.Column(sa.ForeignKey("groups.id"))
    user_uuid = sa.Column(sa.ForeignKey("users.uuid"))
    created_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    last_updated = sa.Column(sa.DateTime, server_default=sa.func.now())
    reserved_start_time = sa.Column(sa.DateTime, nullable=True, server_default=sa.null())
    target_termination_time = sa.Column(sa.DateTime, nullable=True, server_default=sa.null())
    vfolder = sa.Column(sa.ForeignKey("vfolders.id", ondelete="SET NULL"), nullable=True)


class PipelineTask(Base):
    __tablename__ = "pipeline_task"

    id = sa.Column(UUID(as_uuid=True), primary_key=True)
    name = sa.Column(sa.String(length=128))
    icon = sa.Column(sa.String(length=256))
    type = sa.Column(EnumType(PipelineModuleTypes))
    task_spec = sa.Column(JSONB())
    status = sa.Column(EnumType(PipelineTaskStatus))
    last_updated = sa.Column(sa.DateTime, server_default=sa.func.now())
    pipeline_id = sa.Column(sa.ForeignKey("pipeline.id", ondelete="CASCADE"))
    layout_coord_x = sa.Column(sa.Integer())
    layout_coord_y = sa.Column(sa.Integer())
    layout_width = sa.Column(sa.Integer())
    layout_height = sa.Column(sa.Integer())
    input_links = relationship("PipelineTaskInput")
    output_links = relationship("PipelineTaskOutput")
    # TODO: refactor as the session table
    session_id = sa.Column(sa.ForeignKey("kernels.session_id", ondelete="SET NULL"), nullable=True)


class PipelineTaskInput(Base):
    __tablename__ = "pipeline_task_input"

    module_id = sa.Column(sa.ForeignKey("pipeline_task.id", ondelete="CASCADE"), primary_key=True)
    module = relationship("PpipelineTask", back_populates="input_links")
    name = sa.Column(sa.String(length=64), primary_key=True)


class PipelineTaskOutput(Base):
    __tablename__ = "pipeline_task_output"

    module_id = sa.Column(sa.ForeignKey("pipeline_task.id", ondelete="CASCADE"), primary_key=True)
    module = relationship("PpipelineTask", back_populates="output_links")
    name = sa.Column(sa.String(length=64), primary_key=True)


class PipelineTaskLink(Base):
    __tablename__ = "pipeline_task_link"

    pipeline_id = sa.Column(sa.ForeignKey("pipeline.id", ondelete="CASCADE"), primary_key=True)
    task_output_id = sa.Column(sa.ForeignKey("pipeline_task_output.id", ondelete="CASCADE"), primary_key=True)
    task_input_id = sa.Column(sa.ForeignKey("pipeline_task_input.id", ondelete="CASCADE"), primary_key=True)
